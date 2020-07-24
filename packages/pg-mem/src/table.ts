import { IMemoryTable, Schema, SchemaField, DataType, QueryError, RecordExists, TableEvent, ReadOnlyError, NotSupported, IndexDef } from './interfaces';
import { _ISelection, IValue, _ITable, setId, getId, CreateIndexDef, CreateIndexColDef, _IDb, _Transaction, _IQuery, _Column, _IType, CreateColumnDefTyped, _IIndex } from './interfaces-private';
import { buildValue } from './predicate';
import { BIndex } from './btree-index';
import { Selection } from './transforms/selection';
import { parse } from './parser/parser';
import { nullIsh } from './utils';
import { Map as ImMap } from 'immutable';
import { CreateColumnDef, AlterColumn, ColumnConstraint, ConstraintDef } from './parser/syntax/ast';
import { fromNative } from './datatypes';
import { Evaluator } from './valuetypes';
import { ColRef } from './column';
import { Query } from './query';


type Raw<T> = ImMap<string, T>;
export class MemoryTable<T = any> implements IMemoryTable, _ITable<T> {


    private handlers = new Map<TableEvent, Set<() => void>>();

    readonly selection: Selection<T>;
    private it = 0;
    hasPrimary: boolean;
    private readonly: boolean;
    private serials = new Map<string, number>();
    hidden: boolean;
    private dataId = Symbol();
    private indexByHash = new Map<string, BIndex<T>>();
    private indexByName = new Map<string, BIndex<T>>();
    columns: ColRef[] = [];
    columnsByName = new Map<string, ColRef>();

    entropy(t: _Transaction) {
        return this.bin(t).size;
    }

    name: string;

    rename(name: string) {
        const on = this.name;
        if (on === name) {
            return this;
        }
        this.name = name;
        this.schema._doRenTab(on, name);
        return this;
    }

    constructor(readonly schema: _IQuery, t: _Transaction, _schema: Schema) {
        this.name = _schema.name;
        this.selection = new Selection<T>(this, {
            owner: this,
        });

        // fields
        for (const s of _schema.fields) {
            this.addColumn({
                type: s.type as _IType,
                name: s.id,
                constraint: s.primary ? { type: 'primary key' }
                    : s.unique ? { type: 'unique', notNull: s.notNull }
                        : s.notNull ? { type: 'not null' }
                            : null,
                serial: s.autoIncrement,
                default: s.default,
            }, t);
        }

        // auto increments
        for (const s of _schema.fields.filter(x => x.autoIncrement)) {
            this.serials.set(s.id, 0);
        }

        // other table constraints
        for (const c of _schema.constraints ?? []) {
            switch (c.type) {
                case 'primary key':
                    this.createIndex(t, c.columns, 'primary', c.constraintName);
                    break;
                case 'unique':
                    this.createIndex(t, c.columns, 'unique', c.constraintName);
                    break;
                default:
                    throw NotSupported.never(c, 'constraint type');
            }
        }
        this.selection.rebuildColumnIds();
    }

    addColumn(column: CreateColumnDefTyped | CreateColumnDef, t: _Transaction): _Column {
        if ('dataType' in column) {
            const tp = {
                ...column,
                type: fromNative(column.dataType),
            };
            delete tp.dataType;
            return this.addColumn(tp, t);
        }

        const low = column.name.toLowerCase();
        if (this.columnsByName.has(low)) {
            throw new QueryError(`Column "${column.name}" already exists`);
        }
        const cref = new ColRef(this, this.selection.createEvaluator(column.name, column.type));

        if (column.default) {
            cref.alter({
                type: 'set default',
                default: column.default,
                updateExisting: true,
            }, t)
        }

        this.columns.push(cref);
        this.columnsByName.set(low, cref);

        try {
            if (column.constraint) {
                cref.addConstraint(column.constraint, t);
            }
        } catch (e) {
            this.columns.pop();
            this.columnsByName.delete(low);
            throw e;
        }

        // once constraints created, reference them. (constraint creation might have thrown)m
        this.selection.addColumn(cref.expression);
        this.selection.rebuildColumnIds();
    }


    getColumnRef(column: string, nullIfNotFound?: boolean): ColRef {
        const got = this.columnsByName.get(column.toLowerCase());
        if (!got) {
            if (nullIfNotFound) {
                return null;
            }
            throw new QueryError(`Column "${column}" not found`);
        }
        return got;
    }

    bin(t: _Transaction) {
        return t.getMap<Raw<T>>(this.dataId);
    }

    setBin(t: _Transaction, val: Raw<T>) {
        return t.set(this.dataId, val);
    }

    on(event: TableEvent, handler: () => any) {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
    }

    raise(event: TableEvent) {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h();
        }
        this.schema.db.raiseTable(this.name, event);
    }

    setReadonly() {
        this.readonly = true;
        return this;
    }
    setHidden() {
        this.hidden = true;
        return this;
    }


    enumerate(t: _Transaction): Iterable<T> {
        this.raise('seq-scan');
        return this.bin(t).values();
    }

    remapData(t: _Transaction, modify: (newCopy: T) => any) {
        // convert raw data (⚠ must copy the whole thing,
        // because it can throw in the middle of this process !)
        //  => this would result in partially converted tables.
        const converted = this.bin(t).map(x => {
            const copy = { ...x };
            modify(copy);
            return copy;
        });
        this.setBin(t, converted);
    }

    insert(t: _Transaction, toInsert: T, shouldHaveId?: boolean): T {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }

        // get ID of this item
        let newId: string;
        if (shouldHaveId) {
            newId = getId(toInsert);
            if (!newId) {
                throw new Error('Unexpeced update error');
            }
        } else {
            newId = this.name + '_' + (this.it++);
            setId(toInsert, newId);
        }

        // serial (auto increments) columns
        for (const [k, v] of this.serials.entries()) {
            if (!nullIsh(toInsert[k])) {
                continue;
            }
            toInsert[k] = v + 1;
            this.serials.set(k, v + 1);
        }

        // index & check indx contrainsts
        this.indexElt(t, toInsert);

        // set default values
        for (const c of this.columns) {
            c.setDefaults(toInsert, t);
        }

        // check constraints
        for (const c of this.columns) {
            c.checkConstraints(toInsert, t);
        }

        this.setBin(t, this.bin(t).set(newId, toInsert));
        return toInsert;
    }

    update(t: _Transaction, toUpdate: T): T {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }
        this.delete(t, toUpdate);
        return this.insert(t, toUpdate, true);
    }

    delete(t: _Transaction, toDelete: T) {
        const id = getId(toDelete);
        const bin = this.bin(t);
        const got = bin.get(id);
        if (!id || !got) {
            throw new Error('Unexpected error: an operation has been asked on an item which does not belong to this table');
        }

        // remove from indices
        for (const k of this.indexByHash.values()) {
            k.delete(got, t);
        }
        this.setBin(t, bin.delete(id));
        return got;
    }


    private indexElt(t: _Transaction, toInsert: T) {
        for (const k of this.indexByHash.values()) {
            k.add(toInsert, t);
        }
    }

    hasItem(item: T, t: _Transaction) {
        const id = getId(item);
        return this.bin(t).has(id);
    }

    getIndex(forValue: IValue) {
        if (forValue.origin !== this.selection) {
            return null;
        }
        const got = this.indexByHash.get(forValue.hash);
        return got ?? null;
    }


    createIndex(t: _Transaction, expressions: string[] | CreateIndexDef, type?: 'primary' | 'unique', indexName?: string): this {
        if (this.readonly) {
            throw new ReadOnlyError(this.name);
        }
        if (Array.isArray(expressions)) {
            const keys: CreateIndexColDef[] = [];
            for (const e of expressions) {
                const parsed = parse(e, 'expr');
                const getter = buildValue(this.selection, parsed);
                keys.push({
                    value: getter,
                });
            }
            return this.createIndex(t, {
                columns: keys,
                primary: type === 'primary',
                notNull: type === 'primary',
                unique: !!type,
            });
        }

        if (!expressions?.columns?.length) {
            throw new QueryError('Empty index');
        }

        if (expressions.primary && this.hasPrimary) {
            throw new QueryError('Table ' + this.name + ' already has a primary key');
        }
        if (expressions.primary) {
            expressions.notNull = true;
            expressions.unique = true;
        }


        const ihash = expressions.columns.map(x => x.value.hash).sort().join('|');
        const index = new BIndex(t, expressions.columns, this, ihash, indexName ?? expressions.indexName ?? ihash, expressions.unique, expressions.notNull);

        if (this.indexByHash.has(ihash) || this.indexByName.has(index.indexName)) {
            throw new QueryError('Index already exists');
        }

        // fill index (might throw if constraint not respected)
        const bin = this.bin(t);
        for (const e of bin.values()) {
            index.add(e, t);
        }

        // =========== reference index ============
        // ⚠⚠ This must be done LAST, to avoid throwing an execption if index population failed
        for (const col of index.expressions) {
            for (const used of col.usedColumns) {
                this.getColumnRef(used.id).usedInIndexes.add(index);
            }
        }
        this.indexByHash.set(ihash, index);
        this.indexByHash.set(index.indexName, index);
        this.indexByName.set(index.indexName, index)
        if (expressions.primary) {
            this.hasPrimary = true;
        }
        return this;
    }

    dropIndex(u: _IIndex<any>) {
        if (!this.indexByHash.has(u.hash)) {
            throw new QueryError('Cannot drop index that does not belong to this table: ' + u.hash);
        }
        this.indexByHash.delete(u.hash);
        this.indexByName.delete(u.indexName);
    }

    listIndexes(): IndexDef[] {
        return [...this.indexByHash.values()]
            .map<IndexDef>(x => ({
                name: x.indexName,
                expressions: x.expressions.map(x => x.sql),
            }));
    }

    addConstraint(cst: ConstraintDef, t: _Transaction) {
        switch(cst.type) {
            case 'foreign key':
                const ftable = this.schema.getTable(cst.foreignTable);
                const cols = cst.localColumns.map(x => this.getColumnRef(x));
                const fcols = cst.foreignColumns.map(x => ftable.getColumnRef(x));
                if (cols.length !== fcols.length) {
                    throw new QueryError('Foreign key count mismatch');
                }
                if (cst.onDelete !== 'no action' || cst.onUpdate !== 'no action') {
                    throw new NotSupported('Foreign keys with actions not yet supported');
                }
                // todo...
                return;
            default:
                throw NotSupported.never(cst, 'constraint type');
        }
    }
}

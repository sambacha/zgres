import { IQuery, QueryError, SchemaField, DataType, IType, NotSupported } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef } from './interfaces-private';
import { watchUse } from './utils';
import { buildValue } from './predicate';
import { Types } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement, InsertStatement } from './parser/syntax/ast';
import { parse } from './parser/parser';



export class Query implements IQuery {

    constructor(private db: _IDb) {
    }

    none(query: string): void {
        this._query(query);
    }

    many(query: string): any[] {
        return this._query(query);
    }

    private _query(query: string): any[] {



        // see #todo.md

        // query = query.replace(/current_schema\(\)/g, 'current_schema');

        let parsed = parse(query);
        if (!Array.isArray(parsed)) {
            parsed = [parsed];
        }
        let last;
        for (const _p of parsed) {
            if (!_p) {
                continue;
            }
            const p = watchUse(_p);
            switch (p.type) {
                case 'start transaction':
                case 'commit':
                    // ignore those
                    continue;
                case 'rollback':
                    throw new QueryError('Transaction rollback not supported !');
                case 'insert':
                    last = this.executeInsert(p);
                    break;
                // case 'update':
                //     last = this.executeUpdate(p);
                //     break;
                case 'select':
                    last = this.executeSelect(p);
                    break;
                case 'create table':
                    last = this.executeCreateTable(p);
                    break;
                case 'create index':
                    last = this.executeCreateIndex(p);
                    break;
                default:
                    throw NotSupported.never(p, 'statement type');
            }
            p.check?.();
        }
        return last;
    }
    executeCreateIndex(p: any): any {
        if (p.on_kw !== 'on') {
            throw new NotSupported(p.on_kw);
        }
        if (!p.with_before_where) { // what is this ? (always true)
            throw new NotSupported();
        }
        const indexName = p.index;
        const onTable = this.db.getTable(p.table.table);
        const columns = (p.index_columns as any[])
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(onTable.selection, x.column),
                    nullsLast: x.nulls === 'nulls last', // nulls are first by default
                    desc: x.order === 'desc',
                }
            });
        onTable
            .createIndex({
                columns,
                indexName,
            });
    }

    executeCreateTable(p: CreateTableStatement): any {
        // get creation parameters
        const table = p.name;
        if (this.db.getTable(table, true)) {
            throw new QueryError('Table exists: ' + table);
        }

        // perform creation
        this.db.declareTable({
            name: table,
            fields: p.columns
                .map<SchemaField>(f => {
                    let primary = false;
                    let unique = false;
                    let notNull = false;
                    switch (f.constraint?.type) {
                        case 'primary key':
                            primary = true;
                            break;
                        case 'unique':
                            unique = true;
                            notNull = f.constraint.notNull;
                            break;
                        case null:
                        case undefined:
                            break;
                        default:
                            throw NotSupported.never(f.constraint);
                    }

                    const type: IType = (() => {
                        switch (f.dataType.type) {
                            case 'text':
                            case 'varchar':
                                return Types.text(f.dataType.length);
                            case 'int':
                            case 'integer':
                                return Types.int;
                            case 'decimal':
                            case 'float':
                                return Types.float;
                            case 'timestamp':
                                return Types.timestamp;
                            case 'date':
                                return Types.date;
                            case 'json':
                                return Types.json;
                            case 'jsonb':
                                return Types.jsonb;
                            default:
                                throw new NotSupported('Type ' + JSON.stringify(f.dataType));
                        }
                    })();

                    return {
                        id: f.name,
                        type,
                        primary,
                        unique,
                        notNull,
                    }
                })
        });
        return null;
    }

    executeSelect(p: SelectStatement): any[] {
        const t = this.buildSelect(p);
        return [...t.enumerate()];
    }

    buildSelect(p: SelectStatement): _ISelection {
        if (p.type !== 'select') {
            throw new NotSupported(p.type);
        }
        let t: _ISelection;
        const aliases = new Set<string>();
        for (const from of p.from) {
            const alias = from.type === 'table'
                ? from.alias ?? from.table
                : from.alias;
            if (!alias) {
                throw new Error('No alias provided');
            }
            if (aliases.has(alias)) {
                throw new Error(`Table name "${alias}" specified more than once`)
            }
            // find what to select
            let newT = from.type === 'statement'
                ? this.buildSelect(from.statement)
                : this.db.getSchema(from.db).getTable(from.table)
                    .selection;

            // set its alias
            newT = newT.setAlias(alias);

            if (!t) {
                // first table to be selected
                t = newT;
                continue;
            }


            switch (from.join?.type) {
                case 'RIGHT JOIN':
                    t = new JoinSelection(this.db, newT, t, from.join.on, false);
                    break;
                case 'INNER JOIN':
                    t = new JoinSelection(this.db, t, newT, from.join.on, true);
                    break;
                case 'LEFT JOIN':
                    t = new JoinSelection(this.db, t, newT, from.join.on, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }
        t = t.filter(p.where)
            .select(p.columns);
        return t;
    }

    executeUpdate(p: any): any[] {
        throw new Error('Method not implemented.');
    }

    executeInsert(p: InsertStatement): void {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }

        // get table to insert into
        const t = this.db
            .getSchema(p.into.db)
            .getTable(p.into.table);

        // get columns to insert into
        const columns: string[] = p.columns ?? t.selection.columns.map(x => x.id);

        // get values to insert
        if (p.values) {
            const values = p.values;

            for (const val of values) {
                if (val.length !== columns.length) {
                    throw new QueryError('Insert columns / values count mismatch');
                }
                const toInsert = {};
                for (let i = 0; i < val.length; i++) {
                    const notConv = buildValue(null, val[i]);
                    const col = t.selection.getColumn(columns[i]);
                    const converted = notConv.convert(col.type);
                    if (!converted.isConstant) {
                        throw new QueryError('Cannot insert non constant expression');
                    }
                    toInsert[columns[i]] = converted.get(null);
                }
                t.insert(toInsert);
            }
        } else if (p.select) {
            const selection = this.executeSelect(p.select);
            throw new Error('todo: array-mode iteration');
        } else {
            throw new QueryError('Nothing to insert');
        }
    }
}
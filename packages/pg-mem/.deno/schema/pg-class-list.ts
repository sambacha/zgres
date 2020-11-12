import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema, _Transaction } from '../interfaces-private.ts';
import { ReadOnlyError, NotSupported } from '../interfaces.ts';
import { Types, makeArray } from '../datatypes.ts';
import { MAIN_NAMESPACE, SCHEMA_NAMESPACE, parseOid } from './consts.ts';
import { MemoryTable } from '../table.ts';
import { CustomIndex } from './custom-index.ts';
import { ReadOnlyTable } from './readonly-table.ts';

// https://www.postgresql.org/docs/12/catalog-pg-class.html

const IS_SCHEMA = Symbol('_is_pg_classlist');
export class PgClassListTable extends ReadOnlyTable implements _ITable {

    get ownSymbol() {
        return IS_SCHEMA;
    }


    _schema = {
        name: 'pg_class',
        fields: [
            { name: 'oid', type: Types.int } // hidden oid column
            , { name: 'relname', type: Types.text() }
            , { name: 'relnamespace', type: Types.int } // oid
            , { name: 'reltype', type: Types.int } // oid
            , { name: 'reloftype', type: Types.int } // oid
            , { name: 'relowner', type: Types.int } // oid
            , { name: 'relam', type: Types.int } // oid
            , { name: 'relfilenode', type: Types.int } // oid
            , { name: 'reltablespace', type: Types.int } // oid
            , { name: 'relpages', type: Types.int }
            , { name: 'reltyples', type: Types.int }
            , { name: 'relallvisible', type: Types.int }
            , { name: 'reltoastrelid', type: Types.int }
            , { name: 'relhashindex', type: Types.bool }
            , { name: 'relisshared', type: Types.bool }
            , { name: 'relpersistence', type: Types.text(1) } // char(1)
            , { name: 'relkind', type: Types.text(1) } // char(1)
            , { name: 'relnatts', type: Types.int }
            , { name: 'relchecks', type: Types.int }
            , { name: 'relhasoids', type: Types.bool }
            , { name: 'relhasrules', type: Types.bool }
            , { name: 'relhastriggers', type: Types.bool }
            , { name: 'relhassubclass', type: Types.bool }
            , { name: 'relrowsecurity', type: Types.bool }
            , { name: 'relforcerowsecurity', type: Types.bool }
            , { name: 'relispopulated', type: Types.bool }
            , { name: 'relreplident', type: Types.text(1) } // char(1)
            , { name: 'relispartition', type: Types.bool }
            , { name: 'relrewrite', type: Types.int } // oid
            , { name: 'relfrozenxid', type: Types.int } // xid
            , { name: 'relminmxid', type: Types.int } // xid
            , { name: 'relacl', type: Types.text() } // alitem[]
            , { name: 'reloptions', type: makeArray(Types.text()) } // text[]
            , { name: 'relpartbound', type: Types.jsonb } // pg_nod_tr
        ]
    };

    // private indexes: { [key: string]: _IIndex } = {
    //     'oid': new CustomIndex(this, {
    //         get size() {
    //             return this.size
    //         },
    //         column: this.selection.getColumn('oid'),
    //         byColumnValue: (oid: string, t: _Transaction) => {
    //             return [this.byOid(oid, t)]
    //         }
    //     }),
    //     'relname': new CustomIndex(this, {
    //         get size() {
    //             return this.size
    //         },
    //         column: this.selection.getColumn('relname'),
    //         byColumnValue: (oid: string, t: _Transaction) => {
    //             return [this.byRelName(oid, t)];
    //         }
    //     }),
    // }



    private byOid(oid: string, t: _Transaction) {
        const { type, id } = parseOid(oid);
        switch (type) {
            case 'table':
                return this.makeTable(this.schema.getTable(id, true)!);
            case 'index':
                return null;
            // return this.makeTable(this.db.getIndex(id, true));
            default:
                throw NotSupported.never(type);
        }
    }

    private byRelName(name: string, t: _Transaction) {
        return this.schema.getTable(name, true);
        // ?? this.db.getIndex(name, true);
    }

    entropy(t: _Transaction): number {
        return this.schema.tablesCount(t);
    }

    *enumerate() {
        // for (const t of this.db.listTables()) {
        //     yield this.makeTable(t);
        // }
    }


    makeInedx(t: _IIndex<any>): any {
        if (!t) {
            return null;
        }
        // relkind , i = index, S = sequence, t = TOAST table, v = view, m = materialized view, c = composite type, f = foreign table, p = partitioned table, I = partitioned index
        throw new Error('todo');
    }
    makeTable(t: _ITable<any>): any {
        if (!t) {
            return null;
        }
        throw new Error('todo');
        // const ret = {
        //     relname: t.name,
        //     relnamespace: t instanceof MemoryTable
        //         ? MAIN_NAMESPACE
        //         : SCHEMA_NAMESPACE,
        //     relkind: 'r', //  r = ordinary table
        //     [IS_SCHEMA]: true,
        // };
        // return setId(ret, '/schema/pg_class/table/' + t.name);
    }

    hasItem(value: any): boolean {
        return !!value?.[IS_SCHEMA];
    }

}

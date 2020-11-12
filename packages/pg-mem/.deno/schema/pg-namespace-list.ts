import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../interfaces-private.ts';
import { ReadOnlyError, NotSupported, Schema } from '../interfaces.ts';
import { Types } from '../datatypes.ts';
import { ReadOnlyTable } from './readonly-table.ts';

export class PgNamespaceTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_namespace',
        fields: [
            { name: 'oid', type: Types.int } // hidden oid column
            , { name: 'nspname', type: Types.text() }
            , { name: 'nspowner', type: Types.int } // oid
            , { name: 'nspacl', type: Types.jsonb } // aclitem[]
        ]
    };


    entropy(): number {
        return 0;
    }

    *enumerate() {

        // yield {
        //     oid: MAIN_NAMESPACE,
        //     nspname: 'public',
        //     nspowner: null,
        //     nspacl: null,
        // };
        // yield {
        //     oid: MAIN_NAMESPACE,
        //     nspname: 'public',
        //     nspowner: null,
        //     nspacl: null,
        // };
    }



    hasItem(value: any): boolean {
        return false;
    }
}

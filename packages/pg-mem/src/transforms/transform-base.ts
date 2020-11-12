// <== THERE MUST BE NO ACTUAL IMPORTS OTHER THAN IMPORT TYPES (dependency loop)
// ... use 'kind-of' dependency injection below
import type { _ISelection, IValue, _IIndex, _ISchema, _IDb, _Transaction, _SelectExplanation, _Explainer, Stats, nil } from '../interfaces-private';
import type { buildSelection } from './selection';
import type { buildAlias } from './alias';
import type { buildFilter } from './build-filter';
import type { buildGroupBy } from './aggregation';
import type { buildLimit } from './limit';
import type { buildOrderBy } from './order-by';

import { Expr, SelectedColumn, SelectStatement, LimitStatement, OrderByStatement } from '../parser/syntax/ast';
import { RestrictiveIndex } from './restrictive-index';

interface Fns {
    buildSelection: typeof buildSelection;
    buildAlias: typeof buildAlias;
    buildLimit: typeof buildLimit;
    buildFilter: typeof buildFilter;
    buildGroupBy: typeof buildGroupBy;
    buildOrderBy: typeof buildOrderBy;
}
let fns: Fns;
export function initialize(init: Fns) {
    fns = init;
}

export abstract class DataSourceBase<T> implements _ISelection<T> {
    abstract enumerate(t: _Transaction): Iterable<T>;
    abstract entropy(t: _Transaction): number;
    abstract readonly columns: ReadonlyArray<IValue<any>>;
    abstract getColumn(column: string, nullIfNotFound?: boolean): IValue<any>;
    abstract hasItem(value: T, t: _Transaction): boolean;
    abstract getIndex(forValue: IValue): _IIndex<any> | null | undefined;
    abstract explain(e: _Explainer): _SelectExplanation;
    abstract isOriginOf(a: IValue<any>): boolean;
    abstract stats(t: _Transaction): Stats | null;

    constructor(readonly schema: _ISchema) {
    }

    select(select: SelectedColumn[] | nil): _ISelection<any> {
        return fns.buildSelection(this, select);
    }

    filter(filter: Expr | undefined | null): _ISelection {
        if (!filter) {
            return this;
        }
        const plan = fns.buildFilter(this, filter);
        return plan;
    }

    groupBy(grouping: Expr[] | nil, select: SelectedColumn[]): _ISelection {
        if (!grouping?.length) {
            return this;
        }
        const plan = fns.buildGroupBy(this, grouping, select);
        return plan;
    }


    setAlias(alias?: string): _ISelection<any> {
        return fns.buildAlias(this, alias);
    }


    subquery(data: _ISelection<any>, op: SelectStatement): _ISelection {
        // todo: handle refs to 'data' in op statement.
        return this.schema.buildSelect(op);
    }

    limit(limit: LimitStatement): _ISelection {
        if (!limit?.limit && !limit?.offset) {
            return this;
        }
        return fns.buildLimit(this, limit)
    }

    orderBy(orderBy: OrderByStatement[] | nil): _ISelection<any> {
        if (!orderBy?.length) {
            return this;
        }
        return fns.buildOrderBy(this, orderBy);
    }
}

export abstract class TransformBase<T> extends DataSourceBase<T> {


    constructor(protected base: _ISelection) {
        super(base.schema);
    }

    entropy(t: _Transaction): number {
        return this.base.entropy(t);
    }

    isOriginOf(a: IValue<any>): boolean {
        return a.origin === this || this.base?.isOriginOf(a);
    }
}

export abstract class FilterBase<T> extends TransformBase<T> {


    constructor(_base: _ISelection<T>) {
        super(_base);
    }

    get columns(): ReadonlyArray<IValue<any>> {
        return this.base.columns;
    }

    /**
    private _columns: IValue[];
    private _columnMappings: Map<IValue, IValue>;
    get columns(): ReadonlyArray<IValue<any>> {
        this.initCols();
        return this._columns;
        // return this.base.columns;
    }

    private initCols() {
        if (this._columns) {
            return;
        }
        this._columns = [];
        this._columnMappings = new Map();
        for (const c of this.base.columns) {
            const nc = c.setOrigin(this);
            this._columns.push(nc);
            this._columnMappings.set(c, nc);
        }
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        this.initCols();
        const col = this.base.getColumn(column, nullIfNotFound);
        return col && this._columnMappings.get(col);
    }
     */

    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> | nil {
        if (!this.base) { // istanbul ignore next
            throw new Error('Should not call .getColumn() on join');
        }
        if (!('columns' in this.base)) { // istanbul ignore next
            throw new Error('Should not call getColumn() on table');
        }
        return this.base.getColumn(column, nullIfNotFound);
    }

    getIndex(...forValue: IValue<any>[]): _IIndex<any> | null | undefined {
        const index = this.base.getIndex(...forValue);
        if (!index) {
            return null;
        }
        return new RestrictiveIndex(index, this);
    }
}
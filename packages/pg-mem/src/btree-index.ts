import { IValue, _IIndex, _ITable, getId } from './interfaces-private';
import createTree from 'functional-red-black-tree';

type IndexKey = any[];
// https://www.npmjs.com/package/functional-red-black-tree
interface BTree<T> {
    readonly keys: Iterable<IndexKey>;
    readonly values: Iterable<IndexKey>;
    readonly length: number;
    get(key: IndexKey): T;
    insert(key: IndexKey, value: T): BTree<T>;
    remove(key: IndexKey): BTree<T>;
    find(key: IndexKey): BIterator<T>;
    /** Find the first item in the tree whose key is >= key */
    ge(key: IndexKey): BIterator<T>;
    /** Finds the first item in the tree whose key is > key */
    gt(key: IndexKey): BIterator<T>;
    /** Finds the last item in the tree whose key is < key */
    lt(key: IndexKey): BIterator<T>;
    /** Finds the last item in the tree whose key is <= key */
    le(key: IndexKey): BIterator<T>;

    at(pos: number): BIterator<T>;

    readonly begin: BIterator<T>;
    readonly end: BIterator<T>;
    /**  If a truthy value is returned from the visitor, then iteration is stopped. */
    forEach(fn: (key: IndexKey, value: T) => boolean, low?: number, high?: number);
    // root;
}

interface BIterator<T> {
    readonly key: IndexKey;
    readonly value: T;
    // tree;
    // index;
    readonly valid: boolean;
    clone(): BIterator<T>;
    remove(): BTree<T>;
    // update(value: PrimaryKey): BTree;
    next(): void;
    prev(): void;
    readonly hasNext: boolean;
    readonly hasPrev: boolean;
}


export class BIndex<T = any> implements _IIndex<T> {


    private asBinary: BTree<Map<string, T>>;
    private byId = new Set<string>();

    get size() {
        return this.byId.size;
    }

    get entropy() {
        if (!this.asBinary.length) {
            return 0;
        }
        return this.size / this.asBinary.length;
    }


    constructor(readonly expressions: IValue[]
        , readonly onTable: _ITable<T>) {
        this.asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
        this.asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
    }

    private compare(_a: any, _b: any) {
        for (let i = 0; i < this.expressions.length; i++) {
            const k = this.expressions[i];
            const a = _a[i];
            const b = _b[i];
            if (a === null || b === null) {
                if (a === b) {
                    continue;
                }
                return a === null
                    ? -1
                    : 1;
            }
            if (k.equals(a, b)) {
                continue;
            }
            return k.gt(a, b)
                ? 1
                : -1;
        }
        return 0;
    }

    private buildKey(raw: any) {
        return this.expressions.map(k => k.get(raw));
    }

    hasItem(raw: any) {
        const key = getId(raw);
        return this.byId.has(key);
    }

    add(raw: T) {
        const id = getId(raw);
        if (this.byId.has(id)) {
            return;
        }
        const key = this.buildKey(raw);
        let got = this.asBinary.get(key);
        if (!got) {
            this.asBinary = this.asBinary.insert(key, got = new Map());
        }
        if (got.has(id)) {
            return;
        }
        got.set(id, raw);
        this.byId.add(id);
    }

    delete(raw: any) {
        const key = this.buildKey(raw);
        let got = this.asBinary.get(key);
        if (!got) {
            return;
        }
        const id = getId(raw);
        if (!got.has(id)) {
            return;
        }
        this.byId.delete(id);
        got.delete(id);
        if (!got.size) {
            this.asBinary = this.asBinary.remove(key);
        }
    }


    *eq(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            yield* it.value.values();
            it.next();
        }
    }


    *neq(key: IndexKey): Iterable<T> {
        // yield before
        let it = this.asBinary.begin;
        while (it.valid && this.compare(it.key, key) < 0) {
            yield* it.value.values();
            it.next();
        }
        // yield after
        it = this.asBinary.end
        while (it.valid && this.compare(it.key, key) > 0) {
            yield* it.value.values();
            it.prev();
        }
    }

    *gt(key: IndexKey): Iterable<T> {
        const it = this.asBinary.gt(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *ge(key: IndexKey): Iterable<T> {
        const it = this.asBinary.ge(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *lt(key: IndexKey): Iterable<T> {
        const it = this.asBinary.lt(key);
        while (it.valid) {
            yield* it.value.values();
            it.prev();
        }
    }

    *le(key: IndexKey): Iterable<T> {
        const it = this.asBinary.le(key);
        while (it.valid) {
            yield* it.value.values();
            it.prev();
        }
    }
}
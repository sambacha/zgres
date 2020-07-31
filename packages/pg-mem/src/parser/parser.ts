import { Statement, Expr, LOCATION } from './syntax/ast';
import { Parser, Grammar } from 'nearley';
import sqlGrammar from './syntax/main.ne';
import arrayGrammar from './literal-syntaxes/array.ne';
import { QueryError } from '../interfaces-private';
import LRUCache from 'lru-cache';
import hash from 'object-hash';

let sqlCompiled: Grammar;
let arrayCompiled: Grammar;
const astCache = new LRUCache({
    max: 1000,
});

export function parse(sql: string): Statement | Statement[];
export function parse(sql: string, entry: 'expr'): Expr;
export function parse(sql: string, entry?: string): any {
    if (!sqlCompiled) {
        sqlCompiled = Grammar.fromCompiled(sqlGrammar);
    }

    // when 'entry' is not specified, lets cache parsings
    // => better perf on repetitive requests
    const key = !entry && hash(sql);
    if (!entry) {
        const cached = astCache.get(key);
        if (cached) {
            return cached;
        }
    }
    const ret = _parse(sql, sqlCompiled, entry);

    // cache result
    if (!entry) {
        astCache.set(key, ret);
    }
    return ret;
}

export function parseArrayLiteral(sql: string): string[] {
    if (!arrayCompiled) {
        arrayCompiled = Grammar.fromCompiled(arrayGrammar);
    }
    const val = _parse(sql, arrayCompiled);
    return val;
}

function _parse(sql: string, grammar: Grammar, entry?: string): any {
    grammar.start = entry ?? 'main';
    const parser = new Parser(grammar);
    parser.feed(sql);
    const asts = parser.finish();
    if (!asts.length) {
        throw new QueryError('Unexpected end of input');
    } else if (asts.length !== 1) {
        throw new QueryError('Ambiguous syntax: Please file an issue stating the request that has failed:\n' + sql);
    }
    return asts[0];
}

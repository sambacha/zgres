import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { trimNullish } from '../utils';
import { Types } from '../datatypes';
import { preventSeqScan } from './test-utils';
import { IMemoryDb } from '../interfaces';

describe('[Queries] Operators', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    function simpleDb() {
        db.public.declareTable({
            name: 'data',
            fields: [{
                id: 'id',
                type: Types.text(),
                primary: true,
            }, {
                id: 'str',
                type: Types.text(),
            }, {
                id: 'otherStr',
                type: Types.text(),
            }],
        });
        return db;
    }

    it('+ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (1, 2);
                            select a+b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([3]);
    });

    it('- on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 1);
                            select a-b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]);
    });

    it('/ on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([1]); // trunc is used on divisions
    });

    it('/ on neg ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (-17, 10);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([-1]); // trunc is used on divisions
    });

    it('/ on floats', () => {
        const result = many(`create table test(a float, b float);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });
    it('/ on float and int', () => {
        const result = many(`create table test(a float, b int);
                            insert into test values (5, 2);
                            select a/b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([2.5]);
    });

    it('* on ints', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (4, 2);
                            select a*b as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([8]);
    });


    it('respects operator precedence', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select a + b * a as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([6]);
    });


    it('respects parenthesis', () => {
        const result = many(`create table test(a int, b int);
                            insert into test values (2, 2);
                            select (a + b) * a as res from test`);
        expect(result.map(x => x.res)).to.deep.equal([8]);
    });



    describe('IN operators', () => {

        it('"IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3')`);
            const got = many(`select * from data where str in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id1', str: 'str1' }, { id: 'id3', str: 'str3' }]);
        });

        it('"IN" clause with no constant', () => {
            simpleDb();
            none(`insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')`);
            const got = many(`select * from data where id in (str, otherStr)`);
            expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
        });

        it('"IN" clause with constant value', () => {
            simpleDb();
            none("insert into data(id, str, otherStr) values ('A', 'A', 'B'), ('B', 'C', 'D'), ('C', 'A', 'C')");
            const got = many(`select * from data where 'A' in (str, otherStr)`);
            expect(got.map(x => x.id)).to.deep.equal(['A', 'C']);
        });

        it('"NOT IN" clause with constants and no index', () => {
            simpleDb();
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });

        it('"NOT IN" clause with constants index', () => {
            simpleDb();
            db.public.none('create index on data(str)');
            preventSeqScan(db);
            none(`insert into data(id, str) values ('id1', 'str1'), ('id2', 'str2'), ('id3', 'str3'), ('id4', 'str4')`);
            const got = many(`select * from data where str not in ('str1', 'str3')`);
            expect(trimNullish(got)).to.deep.equal([{ id: 'id2', str: 'str2' }, { id: 'id4', str: 'str4' }]);
        });

    })



    it('@> on value query', () => {
        const result = many(`create table test(id text primary key, data jsonb);
                            insert into test values ('id1', '{"prop": "A","in":1}'), ('id2', '{"prop": "B","in":2}'), ('id4', '{"prop": "A","in":3}'), ('id5', null);
                            select id from test where data @> '{"prop": "A"}';`);
        expect(result.map(x => x.id)).to.deep.equal(['id1', 'id4']);
    });

    describe('LIKE operators', () => {

        it('executes like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo%'`))
                .to.deep.equal([
                    { val: 'foo' }
                    , { val: 'foobar' }
                ]);
        });

        it('executes like with _ token', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), (null);
                select * from test where val like 'fo_'`))
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });

        it('executes ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val ilike 'fo%'`))
                .to.deep.equal([
                    { val: 'foo' }
                    , { val: 'foobar' }
                    , { val: 'FOOBAR' }
                ]);
        });


        it('executes pure "startsWith" like with index', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'fo%'`))
                .to.deep.equal([
                    { val: 'foo' }
                    , { val: 'foobar' }
                ]);
        });

        for (const kind of ['asc', 'desc']) {
            it(`executes "startsWith" like with ${kind} index`, () => {
                preventSeqScan(db);
                expect(many(`create table test(val text);
                    create index on test(val ${kind});
                    insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                    select * from test where val like 'fo%b%'`))
                    .to.deep.equal([
                        { val: 'foobar' }
                    ]);
            });
        }
        it('executes startsWith() like with index and _ token', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'fo_'`))
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });

        it('executes like with index without token', () => {
            preventSeqScan(db);
            expect(many(`create table test(val text);
                create index on test(val);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val like 'foo'`))
                .to.deep.equal([
                    { val: 'foo' }
                ]);
        });


        it('executes not like', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not like 'fo%'`))
                .to.deep.equal([
                    { val: 'bar' }
                    , { val: 'FOOBAR' }
                    , { val: null }
                ]);
        });

        it('executes not ilike', () => {
            expect(many(`create table test(val text);
                insert into test values ('foo'), ('bar'), ('foobar'), ('FOOBAR'), (null);
                select * from test where val not ilike 'fo%'`))
                .to.deep.equal([
                    { val: 'bar' }
                    , { val: null }
                ]);
        });
    })





    it('executes array index', () => {
        expect(many(`create table test(val integer[]);
                    insert into test values ('{1, 2, 3}');
                    select val[2] as x from test;`))
            .to.deep.equal([{ x: 2 }]) // <== 1-based !
    });

    it('executes array multiple index', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2][2] as x from test;`))
            .to.deep.equal([{ x: 5 }])
    });

    it('executes array multiple index incomplete indexing', () => {
        expect(many(`create table test(val integer[][]);
                insert into test values ('{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}}');
                select val[2] as x from test;`))
            .to.deep.equal([{ x: null }])
    });

    describe('Between operator', () => {

        for (const x of [
            { query: `select 42 between 1 and 100 as x`, result: { x: true } }
            , { query: `select 101 between 1 and 100 as x`, result: { x: false } }
            , { query: `select 0 between 1 and 100 as x`, result: { x: false } }
            , { query: `select 1 between 1 and 100 as x`, result: { x: true } }
            , { query: `select 100 between 1 and 100 as x`, result: { x: true } }
            , { query: `select '99' between '1' and 100 as x`, result: { x: true } }
            , { query: `select 42 between null and 2 as x`, result: { x: false } }
            , { query: `select 2 between null and 42 as x`, result: { x: null } }
            , { query: `select 42 between 5 and null as x`, result: { x: null } }
            , { query: `select 42 between 100 and null as x`, result: { x: false } }]) {
            it('can select between: ' + x.query, () => {
                expect(many(x.query))
                    .to.deep.equal([x.result])
            });
        }


        for (const x of [
            { query: `select 42 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select 101 not between 1 and 100 as x`, result: { x: true } }
            , { query: `select 0 not between 1 and 100 as x`, result: { x: true } }
            , { query: `select 1 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select 100 not between 1 and 100 as x`, result: { x: false } }
            , { query: `select '99' not between '1' and 100 as x`, result: { x: false } }
            , { query: `select 42 not between null and 2 as x`, result: { x: true } }
            , { query: `select 2 not between null and 42 as x`, result: { x: null } }
            , { query: `select 42 not between 5 and null as x`, result: { x: null } }
            , { query: `select 42 not between 100 and null as x`, result: { x: true } }]) {
            it('can select not between: ' + x.query, () => {
                expect(many(x.query))
                    .to.deep.equal([x.result])
            });
        }

        it('cannot select those betweens', () => {
            assert.throws(() => many(`select 'yo' between '1' and 100 as x`));
            assert.throws(() => many(`select 10 between '1' and 'yo' as x`));
        });


        it('uses index while using between', () => {
            preventSeqScan(db);
            expect(many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num between 1 and 100;`))
                .to.deep.equal([{ num: 1 }
                    , { num: 50 }
                    , { num: 100 }])
        });
        it('uses index while using not between', () => {
            preventSeqScan(db);
            expect(many(`create table test(num integer primary key);
                            insert into test values (0), (1), (50), (100), (101);
                            select * from test where num not between 1 and 100;`))
                .to.deep.equal([{ num: 0 }
                    , { num: 101 }])
        });
    })
});

import moment from 'moment-timezone';
import { UserError } from '../../src/compiler/UserError';
import { PostgresQuery } from '../../src/adapter/PostgresQuery';
import { prepareCompiler } from './PrepareCompiler';
import { MssqlQuery } from '../../src/adapter/MssqlQuery';
import { BaseQuery } from '../../src';
import { createCubeSchema } from './utils';

describe('SQL Generation', () => {
  describe('Common', () => {
    const compilers = /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            every: '10 minute',
          },
        `,
      })
    );

    it('Test time series with different granularity', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [],
      });

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2021-01-01', '2021-01-02']
        });
        expect(timeDimension.timeSeries()).toEqual([
          ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
          ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
        ]);
      }

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2021-01-01', '2021-01-02']
        });
        expect(timeDimension.timeSeries()).toEqual([
          ['2021-01-01T00:00:00.000', '2021-01-01T23:59:59.999'],
          ['2021-01-02T00:00:00.000', '2021-01-02T23:59:59.999']
        ]);
      }

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'cards.createdAt',
          granularity: 'hour',
          dateRange: ['2021-01-01', '2021-01-01']
        });
        expect(timeDimension.timeSeries()).toEqual(
          new Array(24).fill(null).map((v, index) => [
            `2021-01-01T${index.toString().padStart(2, '0')}:00:00.000`,
            `2021-01-01T${index.toString().padStart(2, '0')}:59:59.999`
          ])
        );
      }

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'cards.createdAt',
          granularity: 'minute',
          // for 1 hour only
          dateRange: ['2021-01-01T00:00:00.000', '2021-01-01T00:59:59.999']
        });
        expect(timeDimension.timeSeries()).toEqual(
          new Array(60).fill(null).map((v, index) => [
            `2021-01-01T00:${index.toString().padStart(2, '0')}:00.000`,
            `2021-01-01T00:${index.toString().padStart(2, '0')}:59.999`
          ])
        );
      }

      {
        const timeDimension = query.newTimeDimension({
          dimension: 'cards.createdAt',
          granularity: 'second',
          // for 1 minute only
          dateRange: ['2021-01-01T00:00:00.000', '2021-01-01T00:00:59.000']
        });
        expect(timeDimension.timeSeries()).toEqual(
          new Array(60).fill(null).map((v, index) => [
            `2021-01-01T00:00:${index.toString().padStart(2, '0')}.000`,
            `2021-01-01T00:00:${index.toString().padStart(2, '0')}.999`
          ])
        );
      }
    });

    it('Test for everyRefreshKeySql', async () => {
      await compilers.compiler.compile();

      const timezone = 'America/Los_Angeles';
      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [],
        timezone,
      });
      //
      const utcOffset = moment.tz('America/Los_Angeles').utcOffset() * 60;
      expect(query.everyRefreshKeySql({
        every: '1 hour'
      })).toEqual(['FLOOR((EXTRACT(EPOCH FROM NOW())) / 3600)', false, expect.any(BaseQuery)]);

      // Standard syntax (minutes hours day month dow)
      expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '0 10 * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 36000) / 86400)`, false, expect.any(BaseQuery)]);

      // Additional syntax with seconds (seconds minutes hours day month dow)
      expect(query.everyRefreshKeySql({ every: '0 * * * * *', timezone, }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 60)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '0 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 0) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '30 * * * *', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 1800) / 3600)`, false, expect.any(BaseQuery)]);

      expect(query.everyRefreshKeySql({ every: '30 5 * * 5', timezone }))
        .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - 365400) / 604800)`, false, expect.any(BaseQuery)]);

      for (let i = 1; i < 59; i++) {
        expect(query.everyRefreshKeySql({ every: `${i} * * * *`, timezone }))
          .toEqual([`FLOOR((${utcOffset} + EXTRACT(EPOCH FROM NOW()) - ${i * 60}) / ${1 * 60 * 60})`, false, expect.any(BaseQuery)]);
      }

      try {
        query.everyRefreshKeySql({
          every: '*/9 */7 * * *',
          timezone: 'America/Los_Angeles'
        });

        throw new Error();
      } catch (error) {
        expect(error).toBeInstanceOf(UserError);
      }
    });
  });

  describe('refreshKey from schema', () => {
    const compilers = /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
        refreshKey: {
          every: '10 minute',
        },
      `,
        preAggregations: `
        countCreatedAt: {
            type: 'rollup',
            external: true,
            measureReferences: [count],
            timeDimensionReference: createdAt,
            granularity: \`day\`,
            partitionGranularity: \`month\`,
            refreshKey: {
              every: '1 hour',
            },
            scheduledRefresh: true,
        },
        maxCreatedAt: {
            type: 'rollup',
            external: true,
            measureReferences: [max],
            timeDimensionReference: createdAt,
            granularity: \`day\`,
            partitionGranularity: \`month\`,
            refreshKey: {
              sql: 'SELECT MAX(created_at) FROM cards',
            },
            scheduledRefresh: true,
        },
        minCreatedAt: {
            type: 'rollup',
            external: false,
            measureReferences: [min],
            timeDimensionReference: createdAt,
            granularity: \`day\`,
            partitionGranularity: \`month\`,
            refreshKey: {
              every: '1 hour',
              incremental: true,
            },
            scheduledRefresh: true,
        },
      `
      })
    );

    it('cacheKeyQueries for cube with refreshKey.every (source)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.sum'
        ],
        timeDimensions: [],
        filters: [],
        timezone: 'America/Los_Angeles',
      });

      // Query should not match any pre-aggregation!
      expect(query.cacheKeyQueries()).toEqual([
        [
          // Postgres dialect
          'SELECT FLOOR((EXTRACT(EPOCH FROM NOW())) / 600) as refresh_key',
          [],
          {
            // false, because there is no externalQueryClass
            external: false,
            renewalThreshold: 60,
          }
        ]
      ]);
    });

    it('cacheKeyQueries for cube with refreshKey.every (external)', async () => {
      await compilers.compiler.compile();

      // Query should not match any pre-aggregation!
      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.sum'
        ],
        timeDimensions: [],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      // Query should not match any pre-aggregation!
      expect(query.cacheKeyQueries()).toEqual([
        [
          // MSSQL dialect, because externalQueryClass
          'SELECT FLOOR((DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 600) as refresh_key',
          [],
          {
            // true, because externalQueryClass
            external: true,
            renewalThreshold: 60,
          }
        ]
      ]);
    });

    /**
     * Testing: pre-aggregation which use refreshKey.every & external database defined, should be executed in
     * external database
     */
    it('preAggregationsDescription for query - refreshKey every (external)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          // MSSQL dialect
          'SELECT FLOOR((DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 3600) as refresh_key',
          [],
          {
            external: true,
            renewalThreshold: 300,
          }
        ]
      ]);
    });

    /**
     * Testing: preAggregation which has refresh.sql, should be executed in source db
     */
    it('preAggregationsDescription for query - refreshKey manually (external)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.max'
        ],
        timeDimensions: [],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT MAX(created_at) FROM cards',
          [],
          {
            external: false,
            renewalThreshold: 10,
          }
        ]
      ]);
    });

    it('preAggregationsDescription for query - refreshKey incremental (timeDimensions range)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.min'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT CASE\n    WHEN CURRENT_TIMESTAMP < CAST(@_1 AS DATETIME2) THEN FLOOR((DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 3600) END as refresh_key',
          [
            '__TO_PARTITION_RANGE',
          ],
          {
            external: true,
            incremental: true,
            renewalThreshold: 300,
            renewalThresholdOutsideUpdateWindow: 86400,
            updateWindowSeconds: undefined
          }
        ]
      ]);
    });
  });

  describe('refreshKey only cube (immutable)', () => {
    /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
        refreshKey: {
          immutable: true,
        },
      `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );
  });

  describe('refreshKey only cube (every)', () => {
    const compilers = /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            every: '10 minute',
          },
        `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );

    it('refreshKey from cube (source)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'America/Los_Angeles',
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT FLOOR((EXTRACT(EPOCH FROM NOW())) / 600) as refresh_key',
          [],
          {
            external: false,
            renewalThreshold: 60,
          }
        ]
      ]);
    });

    it('refreshKey from cube (external)', async () => {
      await compilers.compiler.compile();

      const query = new PostgresQuery(compilers, {
        measures: [
          'cards.count'
        ],
        timeDimensions: [{
          dimension: 'cards.createdAt',
          granularity: 'day',
          dateRange: ['2016-12-30', '2017-01-05']
        }],
        filters: [],
        timezone: 'America/Los_Angeles',
        externalQueryClass: MssqlQuery
      });

      const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
      expect(preAggregations.length).toEqual(1);
      expect(preAggregations[0].invalidateKeyQueries).toEqual([
        [
          'SELECT FLOOR((DATEDIFF(SECOND,\'1970-01-01\', GETUTCDATE())) / 600) as refresh_key',
          [],
          {
            external: true,
            renewalThreshold: 60,
          }
        ]
      ]);
    });
  });

  it('refreshKey (sql + every) in cube', async () => {
    const compilers = /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: `
          refreshKey: {
            sql: 'SELECT MAX(created) FROM cards',
            every: '2 hours'
          },
        `,
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
          },
        `
      })
    );
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
      externalQueryClass: MssqlQuery
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
    expect(preAggregations.length).toEqual(1);
    expect(preAggregations[0].invalidateKeyQueries).toEqual([
      [
        'SELECT MAX(created) FROM cards',
        [],
        {
          external: false,
          renewalThreshold: 7200,
        }
      ]
    ]);
  });

  it('refreshKey (sql + every) in preAggregation', async () => {
    const compilers = /** @type Compilers */ prepareCompiler(
      createCubeSchema({
        name: 'cards',
        refreshKey: '',
        preAggregations: `
          countCreatedAt: {
              type: 'rollup',
              external: true,
              measureReferences: [count],
              timeDimensionReference: createdAt,
              granularity: \`day\`,
              partitionGranularity: \`month\`,
              scheduledRefresh: true,
              refreshKey: {
                sql: 'SELECT MAX(created) FROM cards',
                every: '2 hour'
              },
          },
        `
      })
    );
    await compilers.compiler.compile();

    const query = new PostgresQuery(compilers, {
      measures: [
        'cards.count'
      ],
      timeDimensions: [],
      filters: [],
      timezone: 'America/Los_Angeles',
      externalQueryClass: MssqlQuery
    });

    const preAggregations: any = query.newPreAggregations().preAggregationsDescription();
    expect(preAggregations.length).toEqual(1);
    expect(preAggregations[0].invalidateKeyQueries).toEqual([
      [
        'SELECT MAX(created) FROM cards',
        [],
        {
          external: false,
          // 60 * 60 *2
          renewalThreshold: 7200,
        }
      ]
    ]);
  });
});

describe('Class unit tests', () => {
  describe('BaseQuery aliases', () => {
    it('Test BaseQuery with unaliased cube', async () => {
      const set = /** @type Compilers */ prepareCompiler(`
        cube('CamelCaseCube', {
          sql: 'SELECT * FROM TABLE_NAME',
          measures: {
            grant_total: {
              format: 'currency',
              sql: 'grant_field',
              type: 'sum'
            },
          },
          dimensions: {
            id: {
              format: 'id',
              primaryKey: true,
              shown: true,
              sql: 'id',
              type: 'number'
            },
            description: {
              sql: 'description_field',
              type: 'string'
            },
          }
        })
      `);
      await set.compiler.compile();
      const baseQuery = new BaseQuery(set, {});
      // aliasName
      expect(baseQuery.aliasName('CamelCaseCube', false)).toEqual('camel_case_cube');
      expect(baseQuery.aliasName('CamelCaseCube.id', false)).toEqual('camel_case_cube__id');
      expect(baseQuery.aliasName('CamelCaseCube.description', false)).toEqual('camel_case_cube__description');
      expect(baseQuery.aliasName('CamelCaseCube.grant_total', false)).toEqual('camel_case_cube__grant_total');
      
      // aliasName for pre-agg
      expect(baseQuery.aliasName('CamelCaseCube', true)).toEqual('camel_case_cube');
      expect(baseQuery.aliasName('CamelCaseCube.id', true)).toEqual('camel_case_cube_id');
      expect(baseQuery.aliasName('CamelCaseCube.description', true)).toEqual('camel_case_cube_description');
      expect(baseQuery.aliasName('CamelCaseCube.grant_total', true)).toEqual('camel_case_cube_grant_total');
      
      // cubeAlias
      expect(baseQuery.cubeAlias('CamelCaseCube')).toEqual('"camel_case_cube"');
      expect(baseQuery.cubeAlias('CamelCaseCube.id')).toEqual('"camel_case_cube__id"');
      expect(baseQuery.cubeAlias('CamelCaseCube.description')).toEqual('"camel_case_cube__description"');
      expect(baseQuery.cubeAlias('CamelCaseCube.grant_total')).toEqual('"camel_case_cube__grant_total"');
    });
    it('Test BaseQuery with aliased cube', async () => {
      const set = /** @type Compilers */ prepareCompiler(`
        cube('CamelCaseCube', {
          sql: 'SELECT * FROM TABLE_NAME',
          sqlAlias: 'T1',
          measures: {
            grant_total: {
              format: 'currency',
              sql: 'grant_field',
              type: 'sum'
            },
          },
          dimensions: {
            id: {
              format: 'id',
              primaryKey: true,
              shown: true,
              sql: 'id',
              type: 'number'
            },
            description: {
              sql: 'description_field',
              type: 'string'
            },
          }
        })
      `);
      await set.compiler.compile();
      const baseQuery = new BaseQuery(set, {});

      // aliasName
      expect(baseQuery.aliasName('CamelCaseCube', false)).toEqual('t1');
      expect(baseQuery.aliasName('CamelCaseCube.id', false)).toEqual('t1__id');
      expect(baseQuery.aliasName('CamelCaseCube.description', false)).toEqual('t1__description');
      expect(baseQuery.aliasName('CamelCaseCube.grant_total', false)).toEqual('t1__grant_total');
      
      // aliasName for pre-agg
      expect(baseQuery.aliasName('CamelCaseCube', true)).toEqual('t1');
      expect(baseQuery.aliasName('CamelCaseCube.id', true)).toEqual('t1_id');
      expect(baseQuery.aliasName('CamelCaseCube.description', true)).toEqual('t1_description');
      expect(baseQuery.aliasName('CamelCaseCube.grant_total', true)).toEqual('t1_grant_total');

      // cubeAlias
      expect(baseQuery.cubeAlias('CamelCaseCube')).toEqual('"t1"');
      expect(baseQuery.cubeAlias('CamelCaseCube.id')).toEqual('"t1__id"');
      expect(baseQuery.cubeAlias('CamelCaseCube.description')).toEqual('"t1__description"');
      expect(baseQuery.cubeAlias('CamelCaseCube.grant_total')).toEqual('"t1__grant_total"');
    });

    it('Test BaseQuery with the join', async () => {
      const comp = /** @type Compilers */ prepareCompiler(`
        cube('LeftCube', {
          sql: 'SELECT * FROM LEFT_TABLE',
          sqlAlias: 'left',
          measures: {
            total_sum: {
              format: 'currency',
              sql: 'total',
              type: 'sum'
            },
          },
          dimensions: {
            id: {
              format: 'id',
              primaryKey: true,
              shown: true,
              sql: 'id',
              type: 'number'
            },
            description: {
              sql: 'description',
              type: 'string'
            },
          }
        });

        cube('RightCube', {
          sql: 'SELECT * FROM RIGHT_TABLE',
          sqlAlias: 'right',
          measures: {
            total_sum: {
              format: 'currency',
              sql: 'total',
              type: 'sum'
            },
          },
          dimensions: {
            id: {
              format: 'id',
              primaryKey: true,
              shown: true,
              sql: 'id',
              type: 'number'
            },
            description: {
              sql: 'description',
              type: 'string'
            },
          }
        })

        cube('MidCube', {
          sql: 'SELECT * FROM MID_TABLE',
          joins: {
            LeftCube: {
              relationship: 'hasMany',
              sql: \`\${MidCube}.left_id = \${LeftCube}.id\`,
            },
            RightCube: {
              relationship: 'hasMany',
              sql: \`\${MidCube}.right_id = \${RightCube}.id\`,
            },
          },
          dimensions: {
            id: {
              format: 'id',
              primaryKey: true,
              shown: true,
              sql: 'id',
              type: 'number'
            },
          }
        })
      `);
      await comp.compiler.compile();
      const query = new BaseQuery(comp, {
        dimensions: [
          'MidCube.id',
          'LeftCube.description',
        ],
        measures: [
          'RightCube.total_sum',
        ]
      });
      expect(query.buildSqlAndParams()[0]).toEqual(`SELECT  "keys"."mid_cube__id", "keys"."left__description", sum("right_key__right".total) "right__total_sum" FROM (SELECT DISTINCT "right_key__mid_cube".id "mid_cube__id", "right_key__left".description "left__description", "right_key__right".id "right__id" FROM MID_TABLE AS "right_key__mid_cube"
LEFT JOIN RIGHT_TABLE AS "right_key__right" ON "right_key__mid_cube".right_id = "right_key__right".id
LEFT JOIN LEFT_TABLE AS "right_key__left" ON "right_key__mid_cube".left_id = "right_key__left".id ) AS "keys"
LEFT JOIN RIGHT_TABLE AS "right_key__right" ON "keys"."right__id" = "right_key__right".id
              GROUP BY 1, 2  ORDER BY 3 DESC LIMIT 10000`);
    });
  });
});

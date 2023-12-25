# huter

**H**ive **U**nit **TE**st **R**unner

SQL-only unit test framework for Hive!

### Direct Runner

Runs a single hive test, from arguments provided on the cli.

### Repo Runner

Runs tests in a hive repository of scripts.

The idea is to have the least amount of impact on the SQL scripts themselves. Although the default runner expects a
certain structure imposed on the script files and the directories they reside in, you can implement your Runner,
adapted to your desired directory structure.

Huter follows the usual testing frameworks structure (e.g. junit), thus we have:

1. Framework guiding points.
2. Unit to be tested.
3. Test cases.
4. Mocked environment.
5. Test data.
6. Unit evaluation.
7. Assertions.

These elements can be laid out like the following, with their number from list above denoted in front of them:

```
├── co_myCoordinator
│   └── wf_myWorkflow
│       └── the_script_being_tested.hql              # Your script, who we want to test.
├── tables                                           # Table definitions for Hive, by default expected in "tables" dir.
│   ├── sample_table_0.hql
│   └── sample_table_1.hql
└── test/                                            # 1, framework guiding point.
    └── co_myCoordinator/                            # 1
        └── wf_myWorkflow/                           # 1
            └── the_script_being_tested.hql/         # 2, the unit, it's path under "test/" matches the script's path.
                ├── testRejectionOfDuplicates/       # 3, test case.
                │   ├── parameters.ini               # 4, mock env.
                │   ├── setup.hql                    # 5, test data.
                │   ├── test_0.hql                   # 7, assertions.
                │   └── test_1.hql                   # 7
                ├── testSomethingElse/               # 2
                │   └── test_0.hql                   # 7
                ├── parameters.ini                   # 4
                ├── setup.hql                        # 5
                └── dependencies.txt                 # 4
```

Any script prefix with `test_` and suffixed with '.hql' will be treated as a test case.

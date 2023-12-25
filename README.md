# huter
**H**ive **U**nit **TE**st **R**unner

SQL-only unit test framework for Hive!

The idea is to have the least amount of impact on the SQL scripts themselves. Although the default runner expects a certain structure imposed on the script files and the directories they reside in, you can implement your Runner, adapted to your desired directory structure.

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
.
└── test/                                            # 1, framework guiding point.
    └── co_myCoordinator/                            # 1
        └── wf_myWorkflow/                           # 1
            └── the_script_being_tested.hql/         # 2, unit.
                ├── testRejectionOfDuplicates/       # 3, test case.
                │   ├── parameters.ini               # 4, mock env.
                │   ├── setup.hql                    # 5, test data.
                │   ├── test_0.hql                   # 7, assertions.
                │   └── test_1.hql                   # 7
                ├── testSomethingElse/               # 2
                │   └── test_0.hql                   # 7
                ├── parameters.ini                   # 4
                ├── setup.hql                        # 5
                └── table_list.txt                   # 4
```

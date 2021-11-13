# huter
Hive Unit TEst Runner

SQL-only unit test framework for Hive!

The idea is to have the least amount of impact on the SQL scripts themselves. Although the default runner expects a certain structure imposed on the script files and the directories they reside in, you can implement your Runner, adapted to your desired directory structure.

Huter follows the usual testing frameworks structure (e.g. JUnit), thus we have:
- Framework guiding points.
- Unit to be tested.
- Test cases.
- Mocked environment.
- Test data.
- Unit evaluation.
- Assertions.

These elements can be laid out like the following:

```
[name] // this denotes a directory.
<name> // this denotes a file.
(num) // this denotes the element number from the list above. 

[test(1)]
	[co_myCoordinator (1)]
		[wf_myWorkflow (1)]
			[the_script_being_tested.hql (2)]
				<parameters.ini (4)>
				<setup.hql (5)>
				<table_list.txt (4)>
				[testRejectionOfDuplicates (3)]
					<parameters.ini (4)>
					<setup.hql (5)>
					<test_0.hql (7)>
					<test_1.hql (7)>
				[testSomethingElse (3)]
					<test_0.hql (7)>
```

my_root = "/repo"
workflow = "test/"
the_test = "testProperSession"
test_case = "hive_page.hql"


def test_the_script(non_auto_cleaned_huter) -> None:
    """
    This is a pytest sample using the fixture `non_auto_cleaned_huter`,
    which means the output won't be cleaned up and will be available for
    inspection.

    There's no need to close the fixture though, it'll be closed automatically
    and the corresponding JVM shutdown.

    :param non_auto_cleaned_huter: HuterGateway
    :return: None
    """

    # alias it for more readability
    h = non_auto_cleaned_huter

    # there are many options to configure.
    # those beginning with add_... can be specified multiple times.

    # This is where relative table definitions files are resolved from. (corresponds to table_list.txt in Huter)
    h.set_table_definitions_root(my_root)
    h.set_query_file(my_root, test_case)  # This is the main query ran before the test query, but after the setup query.
    h.add_setup_file(my_root, workflow, the_test, "setup.hql")  # this is a setup query just ran before the main query.
    h.set_test_query_file(my_root, workflow, the_test, "test_1.hql")  # test given as a file, generates the final output
    h.add_param_file(my_root, workflow, "parameters.ini")  # file containing parameter substitution for all scripts.
    h.add_table_file(my_root, workflow, "table_list.txt")  # for the syntax of this file, see Huter help on knowledge
    h.set_name("test_the_script")  # some nice name that appears in the output.
    result = h()  # execute everything and get the result.
    print(result)  # result is a dictionary, containing output and errors.
    print(result['output'])
    print(result['errors'])

    output = result['output']
    assert len(output) == 1

    # everything is plain python data.
    # named structs are returned as valid json strings!
    output = output[0]
    assert output == [True, 1, 42, 99]


def test_failing_sample_query(non_auto_cleaned_huter) -> None:
    """
    This test always fails because it has `False` in it's output.

    :param non_auto_cleaned_huter:
    :return:
    """
    h = non_auto_cleaned_huter
    result = h(""" SELECT false as darn, named_struct('haha', 3, 'universe', 42)""")
    print(result)

    # you could use the testing framework from Huter, or you can skip it altogether!
    assert len(result["errors"]) == 0


def test_passing_sample_query(non_auto_cleaned_huter) -> None:
    h = non_auto_cleaned_huter
    result = h(""" SELECT true as hooray, named_struct('haha', 3, 'universe', 42)""")
    print(result)

    # you could use the testing framework from Huter, or you can skip it altogether!
    assert len(result["errors"]) == 0

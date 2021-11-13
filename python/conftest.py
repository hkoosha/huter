import os
import sys
import tempfile
from typing import List, Union

from py4j.java_collections import JavaArray
from py4j.java_gateway import GatewayParameters
from py4j.java_gateway import JavaGateway
from py4j.java_gateway import launch_gateway

try:
    from pytest import fixture
except ModuleNotFoundError:
    # noinspection PyUnusedLocal
    def fixture(arg=None):
        pass

# When launching a JVM, use these options.
JAVA_OPTIONS = ['-Xmx2048m', '-Xms2048m']

# Name of the env var controlling DEFAULT_JAR_DIR
# This is a string value, containing multiple directories separated by os.pathsep.
ENV_VAR__DEFAULT_JAR_DIR = 'HUTER_DIR'
ENV_VAR__SHOW_JVM_OUTPUT = 'HUTER_SHOW_JVM_OUTPUT'

# Default place to look for jars when launching a JVM.
# Controlled by the environmental variable ENV_VAR__DEFAULT_JAR_DIR
DEFAULT_JAR_DIR = ['/opt/huter/lib/']
if ENV_VAR__DEFAULT_JAR_DIR in os.environ:
    DEFAULT_JAR_DIR = os.environ[ENV_VAR__DEFAULT_JAR_DIR].split(os.pathsep)

SHOW_JVM_OUTPUT = False
if ENV_VAR__SHOW_JVM_OUTPUT in os.environ:
    SHOW_JVM_OUTPUT = os.environ[ENV_VAR__SHOW_JVM_OUTPUT].lower() in ["1", "true", "yes"]


def get_entry_point(gateway):
    return gateway.jvm.com.trivago.huter.main.SingleHiveUnitTestRunnerMain


def _join(*path_pars: str) -> str:
    first = True
    for part in path_pars:
        if first:
            first = False
        elif part.startswith('/'):
            raise ValueError("path part starts with slash. "
                             "os.path.join will not work correctly! "
                             "paths: " + str(path_pars))
    return os.path.join(*path_pars)


def _get_class_path(directories: List[str]) -> str:
    """
    Populate the class path string to launch a JVM with. 
    Looks into the given directories (NON RECURSIVELY) and adds all the *.jar
    files in those directories to the class path.
    
    :param directories: The directories to look into (NON RECURSIVELY) for
                        *.jar files.
    :return: A JVM class path string with all the found jar files.
    """
    jars = []
    for d in directories:
        if os.path.isdir(d):
            jars.extend([os.path.join(d, jar) for jar in os.listdir(d) if jar.endswith(".jar")])
    return os.pathsep.join(jars)


def _launch(class_path: str) -> JavaGateway:
    """
    Launch a py4j JVM, and use the given class path.

    :param class_path: the class path to launch the JVM with.
    :return: py4j gateway, connected to the launched JVM.
    """

    if SHOW_JVM_OUTPUT:
        stdout = sys.stdout
        stderr = sys.stderr
    else:
        stdout = None
        stderr = None

    port = launch_gateway(
        javaopts=JAVA_OPTIONS,
        die_on_exit=True,
        classpath=class_path,
        redirect_stdout=stdout,
        redirect_stderr=stderr,
    )

    params = GatewayParameters(
        port=port,
        auto_field=True,
        auto_close=True,
        auto_convert=True
    )

    jvm = JavaGateway(gateway_parameters=params)

    return jvm


def _pre_launched(port: int) -> JavaGateway:
    """
    Found the pre launched py4j jvm and connect to it.

    :param port: the port py4j is running at. if <= 0, use default port.
    :return: Py4j gateway.
    """
    if port <= 0:
        jvm = JavaGateway()
    else:
        params = GatewayParameters(port=port)
        jvm = JavaGateway(gateway_parameters=params)
    return jvm


def _decode_output_result(result) -> dict:
    """
    Convert result from Huter output which is java format into plain python objects.

    :param result: result from Huter output.
    :return: converted output from java objects into python objects.
    """
    return {
        "output": [[j for j in i] for i in result.getOutput()],
        "errors": [i for i in result.getErrors()],
        "huterOutput": result.getHuterOutput(),
    }


class HuterGateway:
    """
    A gateway, through which you can connect to hive and conduct unit tests
    or run queries and get the results back. HuterGateway itself uses a
    Py4j gateway to connect to JVM.

    Example usage:
    h = HuterGateway(gateway=None) # HuterGateway will launch a py4j gateway
    h = HuterGateway(gateway=-1)   # HuterGateway will connect to an already running py4j gateway at default port.
    h = HuterGateway(gateway=42)   # HuterGateway will connect to an already running py4j gateway at port 42.
    h = HuterGateway(gateway=py4j.java_gateway.JavaGateway(...)) # use the given gateway.

    # Run a simple query and get the result:
    result = h('select true, named_struct("foo": "bar")')

    # More advanced example,
    # Run a test case, given the: parameters file, tables file, setup script, test script, query script.
    #   - Note that the paths may contain multiple parts, they are joined with os specific path separator.
    #   - Note that every set_SOMETHING_file has a corresponding method set_SOMETHING (without the file)
    #     where you can directly set the content instead of giving a file for it.

    #   - IMPORTANT -> it's the test query that generates the output! if you do not specify a test query
                       you get empty result.

    h
        # if the table_list.txt refers to a relative file, it is resolved according to this dir
        # See Huter's doc for more info.
        .set_table_definitions_root("/some/path")

        # The query file to run before test query.
        # See Huter's doc for more info.
        .set_query_file("/some/path/the_query_file.sql")

        # The setup file to run before query.
        # See Huter's doc for more info.
        .add_setup_file("/some/path/setup.hql")

        # The test query. THIS QUERY WILL GENERATE THE OUTPUT.
        # See Huter's doc for more info.
        .set_test_query_file(my_root, workflow, the_test, "test_1.hql")

        # The test hive param file.
        # See Huter's doc for more info.
        .add_param_file(my_root, workflow, "parameters.ini")

        # The test tables list.
        # See Huter's doc for more info.
        .add_table_file(my_root, workflow, "table_list.txt")

        # A nice name that appears in the logs.
        .set_name("test_the_script")

    # Finally, after configuring HuterGateway as above:
    result = h()
    print(result) # it's a plain py dict.
    """

    def __init__(self,
                 gateway: Union[JavaGateway, int, None] = None,
                 auto_clean_tmp_dir: bool = True):
        """
        Init

        :param gateway: The py4j gateway to use for connecting to JVM:
         - If it is NONE, a py4j gateway is launched and class path is set to
           what :DEFAULT_JAR_DIR: points to. The :DEFAULT_JAR_DIR: has some
           default value but if the environmental variable 'HUTER_DIR' is set,
           overrides it.
         - Else if it is int, it means to connect to a pre launched py4j gateway at
           this given port. If the value is less than 0, py4j's default port
           will be used.
        - Else, it expects it to be the actual py4j gateway object to use.

        :param auto_clean_tmp_dir: whether if the tmp directory where logs and
            data are put, should be automatically deleted after the gateway is
            closed.
        """

        self.gateway = None
        self._gateway = gateway
        self._is_open = False
        self._is_pre_launched = isinstance(gateway, int)

        self._jar_dir = DEFAULT_JAR_DIR

        self._opts = dict()

        if auto_clean_tmp_dir:
            self._tmp = tempfile.TemporaryDirectory()
            self.tmp = self._tmp.name
            print("tmp dir to be deleted upon exit at: " + self.tmp)
        else:
            self._tmp = None
            self.tmp = tempfile.mkdtemp()
            print("leaving tmp dir undeleted at: " + self.tmp)

        self.set_log_dir(self.tmp, "logs")

    def __call__(self, query: str = None) -> dict:
        return self.run_and_get_output(query)

    def __enter__(self) -> 'HuterGateway':
        self._open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.gateway is not None and not self._is_pre_launched:
            self.gateway.shutdown()

    # --------------------------------------------------------- INTERNAL METHODS

    def _open(self) -> None:
        """
        Ensure underlying gateway is open.

        :return: None
        """

        if self._is_open:
            return
        self._is_open = True

        if self._gateway is None:
            class_path = _get_class_path(self._jar_dir)
            self.gateway = _launch(class_path)
        elif isinstance(self._gateway, int):
            self.gateway = _pre_launched(self._gateway)
        else:
            self.gateway = self._gateway

    def _add_list_option(self, name, value) -> None:
        """
        Add some Huter specific option, where that option is itself a list.
        
        :param name: name of the option.
        :param value: the value to add to the option list.
        :return: None
        """
        if name not in self._opts:
            self._opts[name] = []
        self._opts[name].append(value)

    def _get_root(self) -> str:
        """
        Get root directory where Huter will use as it's working root.

        :return: root directory where Huter will use as it's working root.
        """
        return _join(self.tmp, 'work_root')

    def _get_options(self, params: list = None) -> List[str]:
        """
        Populate Huter options.

        :param params: extra params to add to Huter options, if any.
        :return: a list of Huter options.
        """
        args = ['-r ' + self._get_root()]

        if params is None:
            params = []
        for p in params:
            args.append(p)

        for k, v in self._opts.items():
            if len(k) == 1:
                prefix = '-'
                suffix = ' '
            else:
                prefix = '--'
                suffix = '='
            if type(v) is list:
                for vv in v:
                    args.append(prefix + k + suffix + vv)
            else:
                args.append(prefix + k + suffix + v)

        return args

    def _get_java_options(self, args: List[str]) -> JavaArray:
        """
        Convert Huter options from python's string list to corresponding java array.
        :param args: list of options.
        :return: populated java array.
        """
        java_args = self.gateway.new_array(
            self.gateway.jvm.String, len(args))
        for i, arg in enumerate(args):
            java_args[i] = arg
        return java_args

    def _entry_point(self):
        return get_entry_point(self.gateway)

    def _run(self):
        self._open()
        args = self._get_options()
        j_args = self._get_java_options(args)
        result = self._entry_point().run(j_args)
        return _decode_output_result(result)

    # ---------------------------------------------------------- EXECUTE METHODS

    def run_and_get_output(self, query: str = None) -> dict:
        if query is not None:
            self.set_test_query(query)
        return self._run()

    def run_and_get_output_from_file(self, query_file) -> dict:
        self.set_query_file(query_file)
        return self._run()

    # ---------------------------------------------------------- OPTIONS METHODS

    def set_jar_dirs(self, jar_dirs: List[str]) -> 'HuterGateway':
        """
        Set the directory of jars, where each jar is added to class path of
        the JVM which is about to be launched.

        :param jar_dirs: the directory of jars, where each jar is added to
            class path of the JVM which is about to be launched.
        :raises AssertionError: if gateway is already opened or a pre-launched
            gateway is used.
        :return: self
        """

        if self._is_open or self._is_pre_launched:
            raise AssertionError("gateway is already opened")
        self._jar_dir = jar_dirs
        return self

    def clear_options(self) -> 'HuterGateway':
        """
        Clear all options and start fresh

        :return: self.
        """
        self._opts = dict()
        return self

    def set_root(self, work_root: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param work_root:
        :return:  Self.
        """
        self._opts['root'] = work_root
        return self

    def set_table_definitions_root(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._opts['table-definitions-root'] = _join(*file_)
        return self

    def set_log_dir(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._opts['log-dir'] = _join(*file_)
        return self

    def set_query_file(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._opts['query-file'] = _join(*file_)
        return self

    def set_query(self, query: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param query:
        :return:  Self.
        """
        self._opts['query'] = query
        return self

    def set_test_query_file(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._opts['test-query-file'] = _join(*file_)
        return self

    def set_test_query(self, query: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param query:
        :return:  Self.
        """
        self._opts['test-query'] = query
        return self

    def set_name(self, name: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param name:
        :return:  Self.
        """
        self._opts['name'] = name
        return self

    def add_setup_file(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._add_list_option('setup-file', _join(*file_))
        return self

    def add_setup_query(self, query: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param query:
        :return:  Self.
        """
        self._add_list_option('setup-queries', query)
        return self

    def add_table_file(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._add_list_option('table-file', _join(*file_))
        return self

    def add_table_query(self, query: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param query:
        :return:  Self.
        """
        self._add_list_option('table-queries', query)
        return self

    def add_param_file(self, *file_: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param file_:
        :return:  Self.
        """
        self._add_list_option('param-file', _join(*file_))
        return self

    def add_param_query(self, query: str) -> 'HuterGateway':
        """
        See Huter's doc for more info about this option.

        :param query:
        :return:  Self.
        """
        self._add_list_option('param-query', query)
        return self


@fixture
def auto_cleaned_huter() -> HuterGateway:
    """
    Gives a auto closing (no need to be closed) instance of Huter gateway,
    through which you can connect to hive and conduct unit tests.

    The temp directory created by Huter *WILL* be deleted afterwards.

    The gateway *WILL* launch a JVM itself, and will look for jar files
    to add to class path of JVM at the directories specified by
    :DEFAULT_JAR_DIR: and this property is populated by environmental
    value 'HUTER_DIR' (which if is not set, some default will be used).
    Also look at the method HuterGateway.set_jar_dirs

    :return: A Huter gateway
    """

    with HuterGateway(gateway=None, auto_clean_tmp_dir=True) as huter:
        yield huter


@fixture
def non_auto_cleaned_huter() -> HuterGateway:
    """
    Gives a auto closing (no need to be closed) instance of Huter gateway,
    through which you can connect to hive and conduct unit tests.

    The temp directory created by Huter will *NOT* be deleted afterwards.

    The gateway *WILL* launch a JVM itself, and will look for jar files
    to add to class path of JVM at the directories specified by
    :DEFAULT_JAR_DIR: and this property is populated by environmental
    value 'HUTER_DIR' (which if is not set, some default will be used).
    Also look at the method HuterGateway.set_jar_dirs

    :return: A Huter gateway
    """

    with HuterGateway(gateway=None, auto_clean_tmp_dir=False) as huter:
        yield huter


@fixture
def pre_launched_auto_cleaned_huter() -> HuterGateway:
    """
    Gives a auto closing (no need to be closed) instance of Huter gateway,
    through which you can connect to hive and conduct unit tests.

    The temp directory created by Huter *WILL* be deleted afterwards.

    The gateway will *NOT* launch a JVM, and expects a Py4J ready JVM to
    be launched on the default py4j port.

    :return: A Huter gateway
    """

    with HuterGateway(gateway=-1, auto_clean_tmp_dir=True) as huter:
        yield huter


@fixture
def pre_launched_non_auto_cleaned_huter() -> HuterGateway:
    """
    Gives a auto closing (no need to be closed) instance of Huter gateway,
    through which you can connect to hive and conduct unit tests.

    The temp directory created by Huter will *NOT* be deleted afterwards.

    The gateway will *NOT* launch a JVM, and expects a Py4J ready JVM to
    be launched on the default py4j port.

    :return: A Huter gateway
    """

    with HuterGateway(gateway=-1, auto_clean_tmp_dir=False) as huter:
        yield huter

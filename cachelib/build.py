from pybuilder.core import use_plugin, init, Author

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")
use_plugin("python.flake8")
# use_plugin("python.coverage")
use_plugin("python.distutils")

name = "cachelib"
url = "https://github.com/rustybrooks/pylibs"
summary = "Some python libraries I use"
authors = [Author("Rusty Brooks", "me@rustybrooks.com")]
home_page = "https://github.com/rustybrooks/pylibs"
default_task = "publish"
requires_python = ">=3.7.0"


@init
def set_properties(project):
    project.version = "0.0.2"

    project.depends_on_requirements("requirements.txt")
    project.build_depends_on_requirements("build_requirements.txt")

    # Build and test settings
    project.set_property("flake8_break_build", True)
    project.set_property("flake8_verbose_output", True)
    project.set_property("flake8_include_test_sources", True)
    project.set_property("flake8_max_line_length", 80)
    project.set_property("run_unit_tests_propagate_stdout", True)
    project.set_property("run_unit_tests_propagate_stderr", True)
    # project.set_property("coverage_branch_threshold_warn", 75)
    # project.set_property("coverage_branch_partial_threshold_warn", 50)
    # project.set_property("coverage_exceptions", [])

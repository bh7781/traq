"""
TESTPLAN FUNDAMENTALS AND GETTING STARTED GUIDE
=============================================

1. INSTALLATION & SETUP
----------------------
# Install via pip
pip install testplan

# Basic project structure
my_project/
  ├── tests/
  │   ├── __init__.py
  │   └── test_basic.py
  └── requirements.txt

2. BASIC TEST EXAMPLE
--------------------
"""
from testplan import test_plan
from testplan.testing.multitest import MultiTest, testsuite, testcase


# Define a test suite
@testsuite
class BasicSuite:

    @testcase
    def test_basic_addition(self, env, result):
        """Basic example demonstrating assertions"""
        result.equal(1 + 1, 2, "Basic addition example")
        result.less(5, 10, "Basic comparison example")

    @testcase
    def test_string_operations(self, env, result):
        """String manipulation example"""
        text = "testplan"
        result.true(len(text) == 8, "String length check")
        result.contain("test", text, "Substring check")


# Create the main test plan
@test_plan(name='Basic Example')
def main(plan):
    # Add a test suite to the plan
    plan.add(MultiTest(name='Basic Test', suites=[BasicSuite()]))


"""
3. ADVANCED FEATURES EXAMPLE
---------------------------
Demonstrating fixtures, parametrization, and test environments
"""
from testplan.testing.multitest.driver.tcp import TCPServer, TCPClient
from testplan.common.utils.context import context


@testsuite
class NetworkSuite:
    # Define setup and teardown methods
    def setup(self, env):
        """Set up test environment before each test suite"""
        print("Setting up test suite")

    def teardown(self, env):
        """Clean up after test suite execution"""
        print("Tearing down test suite")

    @testcase
    def test_tcp_communication(self, env, result):
        """Test TCP communication between client and server"""
        # Access server and client from environment
        msg = b"Hello!"
        env.server.accept_connection()
        env.client.send(msg)
        received = env.server.receive(size=len(msg))
        result.equal(received, msg, "TCP communication check")


# Example with environment setup
@test_plan(name='Advanced Example')
def main_advanced(plan):
    # Create MultiTest with custom environment
    test = MultiTest(
        name='Network Test',
        suites=[NetworkSuite()],
        environment=[
            TCPServer(name='server'),
            TCPClient(name='client',
                      host='localhost',
                      port=context('server', '{{port}}'))
        ]
    )
    plan.add(test)


"""
4. PARAMETERIZED TESTING EXAMPLE
-------------------------------
Demonstrating how to create data-driven tests
"""
from testplan.testing.multitest.parametrization import ParametrizedTestCase


@testsuite
class ParameterizedSuite:
    @testcase(parameters=[
        ('Alice', 25),
        ('Bob', 30),
        ('Charlie', 35)
    ])
    def test_person_age(self, env, result, name, age):
        """Parameterized test example"""
        result.greater(age, 0, f"{name}'s age should be positive")
        result.less(age, 150, f"{name}'s age should be reasonable")


"""
5. REPORTING AND ASSERTIONS
--------------------------
Different types of assertions and generating reports
"""


@testsuite
class ReportingSuite:
    @testcase
    def test_various_assertions(self, env, result):
        # Basic assertions
        result.true(True, "Basic boolean check")
        result.false(False, "Negative boolean check")
        result.equal(5, 5, "Equality check")
        result.not_equal(5, 6, "Inequality check")

        # Collection assertions
        result.contain(1, [1, 2, 3], "List membership")
        result.not_contain(4, [1, 2, 3], "Negative list membership")

        # String assertions
        result.regex.match('hello world', r'hello \w+', "Regex pattern matching")

        # Numeric assertions
        result.greater(10, 5, "Greater than")
        result.less(5, 10, "Less than")


"""
COMMON PITFALLS AND BEST PRACTICES
--------------------------------
1. Environment Setup:
   - Always clean up resources in teardown
   - Use context managers for resource management
   - Don't rely on global state between tests

2. Test Independence:
   - Each test should be independent and not rely on other tests
   - Use setup/teardown methods appropriately
   - Avoid sharing mutable state between tests

3. Assertions:
   - Use specific assertions (e.g., result.equal instead of result.true)
   - Include meaningful messages in assertions
   - Don't mix assertions with business logic

4. Test Organization:
   - Group related tests in the same suite
   - Use clear, descriptive names for tests and suites
   - Keep test files organized and maintainable

5. Resource Management:
   - Close connections and files in teardown
   - Use appropriate timeouts for network operations
   - Handle exceptions properly in setup/teardown

TO RUN TESTS:
------------
python test_basic.py
# Or with additional options:
python test_basic.py --pdf report.pdf  # Generate PDF report
python test_basic.py --json report.json  # Generate JSON report
"""
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

if __name__ == "__main__":
    main()
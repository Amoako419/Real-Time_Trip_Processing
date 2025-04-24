import pytest
import os
import sys

# Add the parent directory to the path so pytest can find the script module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Setup environment variables needed for testing
@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    # Set AWS credentials to dummy values for testing
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    
    yield
    
    # Clean up after tests if needed
    pass
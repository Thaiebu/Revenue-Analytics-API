import pytest
import os
import sys
from fastapi.testclient import TestClient

# Add app directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import app

@pytest.fixture
def test_client():
    return TestClient(app)
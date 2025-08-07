import unittest
import json
from unittest.mock import Mock
import azure.functions as func
from function_app import health_check


class TestHealthCheck(unittest.TestCase):
    """Unit tests for the health_check function"""

    def test_health_check_success(self):
        """Test that health_check returns healthy status"""
        req = Mock(spec=func.HttpRequest)
        
        response = health_check(req)
        
        print(response)
        # Assert
        self.assertIsInstance(response, func.HttpResponse)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/json")
        
        # Parse response body
        response_data = json.loads(response.get_body().decode())
        self.assertEqual(response_data["status"], "healthy")

if __name__ == "__main__":
    unittest.main()
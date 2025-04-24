from unittest.mock import patch, MagicMock
import datetime
import ntplib
import pytest
import requests
from metadata_migrate_sync.util import get_utc_time_from_server  # Replace with actual import


# Fixture to freeze UTC time for testing
@pytest.fixture
def fixed_utc_now():
    return datetime.datetime(2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

# Test cases for different API response formats
API_FORMAT_TEST_CASES = [
    pytest.param(
        {'datetime': '2025-04-09T19:42:34.490293+00:00'},
        '2025-04-09T19:39:00.000Z',
        id='worldtimeapi_format'
    ),
    pytest.param(
        {'dateTime': '2025-04-09T19:44:44.0244346'},
        '2025-04-09T19:41:00.000Z',
        id='timeapi_format'
    ),
    pytest.param(
        {'datetime': '2025-04-09T19:42:34.490293Z'},
        '2025-04-09T19:39:00.000Z',
        id='with_Z_suffix'
    ),
]

class TestGetUtcTimeFromServer:
    """Test suite for get_utc_time_from_server()"""

    @patch('ntplib.NTPClient')
    @patch('requests.get')
    def test_ntp_success(self, mock_requests, mock_ntp):
        """Test successful NTP server response"""
        # Setup mock
        mock_response = MagicMock()
        mock_response.tx_time = 1735689600  # Fixed timestamp
        mock_ntp.return_value.request.return_value = mock_response
        
        # Execute
        result = get_utc_time_from_server()
        
        # Verify
        expected_time = (datetime.datetime.fromtimestamp(1735689600, datetime.timezone.utc) - 
                       datetime.timedelta(minutes=3)).replace(second=0, microsecond=0)
        expected_str = expected_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        
        assert result == expected_str
        mock_requests.assert_not_called()

    @patch('ntplib.NTPClient')
    @patch('requests.get')
    def test_ntp_fallback_to_first_api(self, mock_requests, mock_ntp):
        """Test NTP failure with fallback to first API"""
        # Setup mocks
        mock_ntp.return_value.request.side_effect = ntplib.NTPException("NTP error")
        mock_api_response = MagicMock()
        mock_api_response.json.return_value = {'datetime': '2025-04-09T19:44:44.024434+00:00'}
        mock_requests.return_value = mock_api_response
        
        test_time = datetime.datetime(2025, 4, 9, 19, 44, 44, 24434, tzinfo=datetime.timezone.utc)
        # Execute
        # Mock datetime.fromisoformat to return our fixed time
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = test_time
            mock_datetime.fromisoformat.return_value = test_time
            result = get_utc_time_from_server()
        
        # Verify
        expected_time = (datetime.datetime.fromisoformat('2025-04-09T19:44:44.024434+00:00') - 
                       datetime.timedelta(minutes=3)).replace(second=0, microsecond=0)
        expected_str = expected_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        
        assert result == expected_str
        mock_requests.assert_called_once_with("http://worldtimeapi.org/api/timezone/Etc/UTC", timeout=5)

    @patch('ntplib.NTPClient')
    @patch('requests.get')
    def test_all_apis_fallback_to_local(self, mock_requests, mock_ntp, fixed_utc_now):
        """Test all API failures fall back to local time"""
        # Setup mocks
        mock_ntp.return_value.request.side_effect = ntplib.NTPException("NTP error")
        mock_requests.side_effect = requests.RequestException("API error")
        
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = fixed_utc_now
            mock_datetime.fromtimestamp.return_value = fixed_utc_now
            mock_datetime.fromisoformat.return_value = fixed_utc_now
            
            # Execute
            result = get_utc_time_from_server()
        
        # Verify
        expected_time = (fixed_utc_now - datetime.timedelta(minutes=3)).replace(second=0, microsecond=0)
        expected_str = expected_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        
        assert result == expected_str
        assert mock_requests.call_count == 3  # Should try all 3 APIs

    @patch('ntplib.NTPClient')
    @patch('requests.get')
    def test_custom_ahead_minutes(self, mock_requests, mock_ntp):
        """Test with custom ahead_minutes parameter"""
        # Setup mock
        mock_response = MagicMock()
        mock_response.tx_time = 1735689600  # Fixed timestamp
        mock_ntp.return_value.request.return_value = mock_response
        
        # Execute with custom minutes
        result = get_utc_time_from_server(ahead_minutes=5)
        
        # Verify
        expected_time = (datetime.datetime.fromtimestamp(1735689600, datetime.timezone.utc) - 
                       datetime.timedelta(minutes=5)).replace(second=0, microsecond=0)
        expected_str = expected_time.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        
        assert result == expected_str

    @patch('ntplib.NTPClient')
    @patch('requests.get')
    @pytest.mark.parametrize("api_response,expected", API_FORMAT_TEST_CASES)
    def test_datetime_format_handling(self, mock_requests, mock_ntp, api_response, expected):
        """Test different datetime formats from APIs (parametrized)"""
        # Setup mocks
        mock_ntp.return_value.request.side_effect = ntplib.NTPException("NTP error")
        mock_api_response = MagicMock()
        mock_api_response.json.return_value = api_response
        mock_requests.return_value = mock_api_response
        
        # Execute
        result = get_utc_time_from_server()
        
        # Verify
        assert result == expected

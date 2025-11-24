"""
Authentication Service for MCP SQL Server
Simplified version for internal VPN use - always returns "system" user
No JWT or SSO integration - relies on network-level security
"""
from typing import Dict, Any


class AuthService:
    """Service for handling authentication - simplified for internal use."""
    
    def __init__(self):
        """Initialize authentication service."""
        self.enabled = False  # No authentication - VPN network security only
    
    def get_user_id(self) -> str:
        """
        Get the current user ID.
        For internal VPN deployment, always returns "system".
        
        Returns:
            User ID string (always "system")
        """
        return "system"
    
    def get_authentication_info(self) -> Dict[str, Any]:
        """
        Get authentication configuration information.
        
        Returns:
            Dictionary with authentication status
        """
        return {
            "enabled": False,
            "type": "VPN Network Security",
            "user_tracking": "All operations logged as 'system' user",
            "message": "Authentication disabled - relying on VPN network isolation"
        }


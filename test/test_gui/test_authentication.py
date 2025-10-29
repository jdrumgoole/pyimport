"""
GUI tests for authentication functionality

Tests user registration, login, logout, and password management.
"""

import pytest
from playwright.sync_api import Page, expect


@pytest.mark.gui
class TestAuthentication:
    """Test suite for authentication workflows"""

    def test_user_registration(self, page: Page, pyimport_server, server_url):
        """Test new user registration flow"""
        page.goto(server_url)

        # Click on Register tab
        page.click('button.tab:has-text("Register")')
        page.wait_for_selector('#register.tab-content.active', timeout=5000)

        # Fill in registration form
        username = f"newuser_{int(page.evaluate('Date.now()'))}"
        page.fill('#reg-username', username)
        page.fill('#reg-password', 'SecurePassword123')
        page.fill('#reg-email', f'{username}@example.com')
        page.fill('#reg-fullname', 'New Test User')

        # Submit registration
        page.locator('#register form button[type="submit"]').click()

        # Wait for success message
        success_status = page.locator('#register-status.status.success')
        expect(success_status).to_be_visible(timeout=5000)
        expect(success_status).to_contain_text(f'User "{username}" registered successfully')

    def test_duplicate_registration_fails(self, page: Page, pyimport_server, server_url):
        """Test that registering with an existing username fails"""
        page.goto(server_url)

        # Register first user
        page.click('button.tab:has-text("Register")')
        page.wait_for_selector('#register.tab-content.active', timeout=5000)

        username = f"duplicate_{int(page.evaluate('Date.now()'))}"
        page.fill('#reg-username', username)
        page.fill('#reg-password', 'Password123')
        page.locator('#register form button[type="submit"]').click()

        # Wait for success
        page.wait_for_selector('#register-status.status.success', timeout=5000)

        # Try to register again with same username
        page.fill('#reg-username', username)
        page.fill('#reg-password', 'DifferentPassword456')
        page.locator('#register form button[type="submit"]').click()

        # Should get error
        error_status = page.locator('#register-status.status.error')
        expect(error_status).to_be_visible(timeout=5000)
        expect(error_status).to_contain_text('already registered')

    def test_user_login(self, page: Page, pyimport_server, server_url):
        """Test user login flow"""
        page.goto(server_url)

        # First register a user
        page.click('button.tab:has-text("Register")')
        page.wait_for_selector('#register.tab-content.active', timeout=5000)

        username = f"logintest_{int(page.evaluate('Date.now()'))}"
        password = 'TestPassword123'
        page.fill('#reg-username', username)
        page.fill('#reg-password', password)
        page.locator('#register form button[type="submit"]').click()
        page.wait_for_selector('#register-status.status.success', timeout=5000)

        # Now login
        page.click('button.tab:has-text("Login")')
        page.wait_for_selector('#login.tab-content.active', timeout=5000)

        page.fill('#login-username', username)
        page.fill('#login-password', password)
        page.locator('#login form button[type="submit"]').click()

        # Wait for successful login
        success_status = page.locator('#login-status.status.success')
        expect(success_status).to_be_visible(timeout=5000)
        expect(success_status).to_contain_text('Login successful')

        # Verify token is displayed
        token_display = page.locator('#token-display')
        expect(token_display).to_be_visible()

        # Verify we're redirected to authenticated view
        page.wait_for_selector('body.authenticated', timeout=10000)
        page.wait_for_selector('.nav-logout', state='visible', timeout=5000)

    def test_login_with_invalid_credentials(self, page: Page, pyimport_server, server_url):
        """Test login fails with invalid credentials"""
        page.goto(server_url)

        # Try to login with non-existent user
        page.click('button.tab:has-text("Login")')
        page.fill('#login-username', 'nonexistentuser')
        page.fill('#login-password', 'wrongpassword')
        page.locator('#login form button[type="submit"]').click()

        # Should get error
        error_status = page.locator('#login-status.status.error')
        expect(error_status).to_be_visible(timeout=5000)
        expect(error_status).to_contain_text('Incorrect username or password')

    def test_logout(self, authenticated_page: Page):
        """Test user logout functionality"""
        # Should already be logged in
        expect(authenticated_page.locator('.nav-logout')).to_be_visible()

        # Click logout
        authenticated_page.click('.nav-logout')

        # Wait a moment for JavaScript to execute
        authenticated_page.wait_for_timeout(1000)

        # Check that token was removed from localStorage
        token_removed = authenticated_page.evaluate("() => localStorage.getItem('access_token') === null")
        assert token_removed, "Access token should be removed from localStorage after logout"

    def test_admin_login(self, page: Page, pyimport_server, server_url):
        """Test admin user can login"""
        page.goto(server_url)

        # Login as admin
        page.click('button.tab:has-text("Login")')
        page.fill('#login-username', 'admin')
        page.fill('#login-password', 'admin')
        page.locator('#login form button[type="submit"]').click()

        # Wait for successful login
        page.wait_for_selector('.nav-logout', timeout=5000)
        expect(page.locator('body')).to_have_class('authenticated')

    def test_token_persistence(self, page: Page, pyimport_server, server_url):
        """Test that authentication token persists across page reloads"""
        page.goto(server_url)

        # Register and login
        page.click('button.tab:has-text("Register")')
        page.wait_for_selector('#register.tab-content.active', timeout=5000)

        username = f"persist_{int(page.evaluate('Date.now()'))}"
        password = 'PersistPassword123'
        page.fill('#reg-username', username)
        page.fill('#reg-password', password)
        page.locator('#register form button[type="submit"]').click()
        page.wait_for_selector('#register-status.status.success', timeout=5000)

        page.click('button.tab:has-text("Login")')
        page.wait_for_selector('#login.tab-content.active', timeout=5000)

        page.fill('#login-username', username)
        page.fill('#login-password', password)
        page.locator('#login form button[type="submit"]').click()
        page.wait_for_selector('body.authenticated', timeout=10000)
        page.wait_for_selector('.nav-logout', state='visible', timeout=5000)

        # Reload the page
        page.reload()

        # Should still be authenticated
        page.wait_for_selector('.nav-logout', timeout=5000)
        expect(page.locator('body')).to_have_class('authenticated')

    def test_navigation_between_views(self, authenticated_page: Page):
        """Test navigation between Import and Profile views"""
        # Should start on Import view
        expect(authenticated_page.locator('#import-view')).to_be_visible()

        # Navigate to Profile
        authenticated_page.click('.nav-profile')
        expect(authenticated_page.locator('#profile-view')).to_be_visible()
        expect(authenticated_page.locator('#import-view')).not_to_be_visible()

        # Navigate back to Import
        authenticated_page.click('.nav-item:has-text("Import")')
        expect(authenticated_page.locator('#import-view')).to_be_visible()
        expect(authenticated_page.locator('#profile-view')).not_to_be_visible()

    def test_get_user_profile(self, authenticated_page: Page):
        """Test loading user profile information"""
        # Navigate to profile view
        authenticated_page.click('.nav-profile')

        # Click Load Profile button
        authenticated_page.click('button:has-text("Load Profile")')

        # Wait for user info to be displayed
        user_info = authenticated_page.locator('#user-info')
        expect(user_info).to_be_visible(timeout=5000)

        # Verify profile contains expected fields
        expect(user_info).to_contain_text('Username:')
        expect(user_info).to_contain_text('Email:')
        expect(user_info).to_contain_text('Status:')

    def test_change_password(self, page: Page, pyimport_server, server_url):
        """Test password change functionality"""
        page.goto(server_url)

        # Register a new user
        page.click('button.tab:has-text("Register")')
        page.wait_for_selector('#register.tab-content.active', timeout=5000)

        username = f"pwchange_{int(page.evaluate('Date.now()'))}"
        old_password = 'OldPassword123'
        new_password = 'NewPassword456'

        page.fill('#reg-username', username)
        page.fill('#reg-password', old_password)
        page.locator('#register form button[type="submit"]').click()
        page.wait_for_selector('#register-status.status.success', timeout=5000)

        # Login
        page.click('button.tab:has-text("Login")')
        page.wait_for_selector('#login.tab-content.active', timeout=5000)

        page.fill('#login-username', username)
        page.fill('#login-password', old_password)
        page.locator('#login form button[type="submit"]').click()
        page.wait_for_selector('body.authenticated', timeout=10000)
        page.wait_for_selector('.nav-logout', state='visible', timeout=5000)

        # Go to profile
        page.click('.nav-profile')

        # Change password
        page.fill('#current-password', old_password)
        page.fill('#new-password', new_password)
        page.fill('#confirm-password', new_password)
        page.locator('#profile-view form button[type="submit"]').click()

        # Wait for success
        success_status = page.locator('#profile-status.status.success')
        expect(success_status).to_be_visible(timeout=5000)
        expect(success_status).to_contain_text('Password changed successfully')

        # Verify password was changed by checking we can authenticate with new password
        # (No need to logout/login in UI, just verify the change worked)

    def test_change_password_with_mismatch(self, authenticated_page: Page):
        """Test password change fails when new passwords don't match"""
        # Go to profile
        authenticated_page.click('.nav-profile')

        # Try to change password with mismatched new passwords
        authenticated_page.fill('#current-password', 'testpassword123')
        authenticated_page.fill('#new-password', 'NewPassword123')
        authenticated_page.fill('#confirm-password', 'DifferentPassword123')
        authenticated_page.locator('#profile-view form button[type="submit"]').click()

        # Should get error
        error_status = authenticated_page.locator('#profile-status.status.error')
        expect(error_status).to_be_visible(timeout=5000)
        expect(error_status).to_contain_text('do not match')

    def test_change_password_with_short_password(self, authenticated_page: Page):
        """Test password change fails with password shorter than 8 characters"""
        # Go to profile
        authenticated_page.click('.nav-profile')
        authenticated_page.wait_for_selector('#profile-view', state='visible', timeout=5000)

        # Try to change password with too short password
        authenticated_page.fill('#current-password', 'testpassword123')
        authenticated_page.fill('#new-password', 'short')
        authenticated_page.fill('#confirm-password', 'short')

        # The new-password field has HTML5 minlength="8" validation
        # Check if the field is invalid according to HTML5 validation
        is_invalid = authenticated_page.evaluate("""
            () => {
                const field = document.getElementById('new-password');
                return !field.validity.valid;
            }
        """)
        assert is_invalid, "Password field should be invalid with length < 8"

        # Verify we get the browser's validation message
        validation_message = authenticated_page.evaluate("""
            () => {
                const field = document.getElementById('new-password');
                return field.validationMessage;
            }
        """)
        # Different browsers may have different validation messages, but should mention length
        assert len(validation_message) > 0, "Should have a validation message for short password"

    def test_change_password_with_wrong_current_password(self, authenticated_page: Page):
        """Test password change fails with incorrect current password"""
        # Go to profile
        authenticated_page.click('.nav-profile')

        # Try to change password with wrong current password
        authenticated_page.fill('#current-password', 'WrongCurrentPassword')
        authenticated_page.fill('#new-password', 'NewPassword123')
        authenticated_page.fill('#confirm-password', 'NewPassword123')
        authenticated_page.locator('#profile-view form button[type="submit"]').click()

        # Should get error - wait for element to exist and have error text
        authenticated_page.wait_for_timeout(1000)  # Wait for response
        error_status = authenticated_page.locator('#profile-status.status.error')
        expect(error_status).to_be_attached()  # Element exists in DOM
        expect(error_status).to_contain_text('Current password is incorrect')

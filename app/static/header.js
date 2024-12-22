// Cookie utility functions
function getCookie(name) {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return parts.pop().split(';').shift();
    return null;
}

function deleteCookie(name) {
    document.cookie = `${name}=; path=/; expires=Thu, 01 Jan 1970 00:00:01 GMT; Secure; SameSite=Strict`;
}

function getUserInfo() {
    // Check for initial server-provided data first
    if (window.initialUserData) {
        return window.initialUserData;
    }
    // Fall back to localStorage if needed
    const userInfoStr = localStorage.getItem('user_info');
    if (userInfoStr) {
        try {
            return JSON.parse(userInfoStr);
        } catch (e) {
            console.error('Error parsing user info:', e);
        }
    }
    return null;
}

function updateUIForAuthState() {
    const accessToken = getCookie('access_token_cookie');
    const userInfo = getUserInfo();

    const loginButton = document.getElementById('loginButton');
    const userMenu = document.getElementById('userMenu');
    const userMenuButton = document.getElementById('userMenuButton');

    if (!loginButton || !userMenu || !userMenuButton) {
        console.error('Required DOM elements not found');
        return;
    }

    if (accessToken && userInfo) {
        console.log('Updating UI for logged in user:', userInfo);
        loginButton.classList.add('hidden');
        userMenu.classList.remove('hidden');

        // Find the email span within the button and update it
        const emailSpan = userMenuButton.querySelector('.user-email');
        if (emailSpan) {
            emailSpan.textContent = userInfo.email;
        } else {
            console.error('Email span not found in user menu button');
        }
    } else {
        console.log('User not logged in');
        loginButton.classList.remove('hidden');
        userMenu.classList.add('hidden');
    }
}

function loginWith(provider) {
    const currentUrl = encodeURIComponent(window.location.href);
    window.location.href = `/login/${provider}?next=${currentUrl}`;
}

function logout() {
    fetch('/logout', {
        method: 'POST',
        credentials: 'include'
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Logout failed');
        }
        return response.json();
    })
    .then(() => {
        // Clear cookies and local storage
        deleteCookie('access_token_cookie');
        deleteCookie('refresh_token_cookie');
        localStorage.removeItem('user_info');
        window.initialUserData = null;
        // Redirect to home page
        window.location.href = '/';
    })
    .catch(error => {
        console.error('Logout error:', error);
        alert('Failed to logout. Please try again.');
    });
}

function navigateToChatbot(region) {
    const accessToken = getCookie('access_token_cookie');
    if (accessToken) {
        const langSelector = document.getElementById('languageSelector');
        const currentLang = langSelector ? langSelector.value : 'en';
        window.location.href = `/chatbot/${region}?lang=${currentLang}`;
    } else {
        alert('Please log in to access the chatbot.');
        document.getElementById('loginModal').classList.remove('hidden');
    }
}

// Initialize everything when the DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    // Get DOM elements
    const loginButton = document.getElementById('loginButton');
    const loginModal = document.getElementById('loginModal');
    const closeModal = document.getElementById('closeModal');
    const languageSelector = document.getElementById('languageSelector');
    const userMenu = document.getElementById('userMenu');
    const userMenuButton = document.getElementById('userMenuButton');
    const userMenuContent = document.getElementById('userMenuContent');
    const logoutButton = document.getElementById('logoutButton');

    // Initialize user data if available from server
    if (window.initialUserData) {
        localStorage.setItem('user_info', JSON.stringify(window.initialUserData));
    }

    // Handle login modal
    if (loginButton && loginModal && closeModal) {
        loginButton.addEventListener('click', () => {
            loginModal.classList.remove('hidden');
        });

        closeModal.addEventListener('click', () => {
            loginModal.classList.add('hidden');
        });

        // Close modal when clicking outside
        window.addEventListener('click', (event) => {
            if (event.target === loginModal) {
                loginModal.classList.add('hidden');
            }
        });
    }

    // Handle language selection
    if (languageSelector) {
        languageSelector.addEventListener('change', function() {
            window.location.href = '/?lang=' + this.value;
        });
    }

    // Handle user menu dropdown
    if (userMenuButton && userMenuContent) {
        userMenuButton.addEventListener('click', (e) => {
            e.stopPropagation();
            userMenuContent.classList.toggle('hidden');
        });

        // Close dropdown when clicking outside
        document.addEventListener('click', (event) => {
            if (!userMenu.contains(event.target)) {
                userMenuContent.classList.add('hidden');
            }
        });
    }

    // Handle logout button
    if (logoutButton) {
        logoutButton.addEventListener('click', logout);
    }

    // Handle login response from OAuth
    function handleLoginResponse() {
        const urlParams = new URLSearchParams(window.location.search);
        const userInfo = urlParams.get('user_info');

        if (userInfo) {
            try {
                const decodedUserInfo = decodeURIComponent(userInfo);
                localStorage.setItem('user_info', decodedUserInfo);
                // Clean URL
                window.history.replaceState({}, document.title, window.location.pathname);
            } catch (e) {
                console.error('Error handling login response:', e);
            }
        }
    }

    // Initialize UI state
    handleLoginResponse();
    updateUIForAuthState();

    // Handle protected links
    const protectedLinks = document.querySelectorAll('[data-protected="true"]');
    protectedLinks.forEach(link => {
        link.addEventListener('click', function(event) {
            if (!getCookie('access_token_cookie')) {
                event.preventDefault();
                alert('Please log in to access this feature.');
                loginModal.classList.remove('hidden');
            }
        });
    });

    // Auto-hide flash messages
    const flashMessages = document.querySelectorAll('.flash-message');
    flashMessages.forEach(message => {
        setTimeout(() => {
            message.style.opacity = '0';
            setTimeout(() => {
                message.style.display = 'none';
            }, 300);
        }, 5000);
    });
});

// Intercept all fetch requests to include credentials
const originalFetch = window.fetch;
window.fetch = function() {
    if (!arguments[1]) {
        arguments[1] = {};
    }
    arguments[1].credentials = 'include';
    return originalFetch.apply(this, arguments);
};
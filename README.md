# 🐍 Insighta Labs API
This is the central authentication and data engine for the Insighta ecosystem. It handles user synchronization via GitHub OAuth and manages sessions for both web and terminal-based clients.

### 🚀 Key Features
*   **Hybrid JWT Authentication**: Simultaneously supports HTTP-only cookies for web security and Bearer tokens for CLI access.
*   **Dual-Flow OAuth**: A smart callback handler that detects the request source (`web` vs `cli`) to deliver credentials appropriately.
*   **UUIDv7 Identity**: Utilizes `uuid6` for time-ordered, database-efficient user identification.
*   **Security**: Integrated rate limiting and CSRF protection for sensitive endpoints.

### 🛠️ Local Setup
1.  **Environment**: Create a `.env` file with your `JWT_SECRET_KEY`, `DATABASE_URL`, and GitHub OAuth credentials.
2.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run Application**:
    ```bash
    python app_2.py
    ```

### 📡 Core Endpoints
*   `GET /auth/github`: Initiates the OAuth handshake.
*   `GET /auth/github/callback`: Processes the code and redirects based on `state`.
*   `GET /api/me`: Returns the authenticated user's profile.
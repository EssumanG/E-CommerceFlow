from app import create_app
import os

app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))  # Ensure it uses the correct port
    app.run(host="0.0.0.0", port=port)
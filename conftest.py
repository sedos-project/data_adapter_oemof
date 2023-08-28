import pathlib

import dotenv

TEST_ENV_FILE = pathlib.Path(__file__).parent / "tests" / ".env"
dotenv.load_dotenv(TEST_ENV_FILE)

"""Seed default user and assign existing races

Revision ID: 004
Revises: 003
Create Date: 2026-03-15

"""
from typing import Sequence, Union
import uuid

from alembic import op
import sqlalchemy as sa
from passlib.context import CryptContext


# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEFAULT_USER_EMAIL = "blort.music@gmail.com"
DEFAULT_USER_PASSWORD = "changeme1234"
DEFAULT_USER_ID = str(uuid.uuid4())


def upgrade() -> None:
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    hashed = pwd_context.hash(DEFAULT_USER_PASSWORD)

    # Insert default user
    op.execute(
        sa.text(
            "INSERT INTO users (id, email, hashed_password, created_at) "
            "VALUES (:id, :email, :hashed_password, NOW())"
        ).bindparams(
            id=DEFAULT_USER_ID,
            email=DEFAULT_USER_EMAIL,
            hashed_password=hashed,
        )
    )

    # Assign all existing races to the default user
    op.execute(
        sa.text(
            "UPDATE races SET user_id = :user_id WHERE user_id IS NULL"
        ).bindparams(user_id=DEFAULT_USER_ID)
    )


def downgrade() -> None:
    # Unassign races from default user
    op.execute(
        sa.text(
            "UPDATE races SET user_id = NULL WHERE user_id = "
            "(SELECT id FROM users WHERE email = :email)"
        ).bindparams(email=DEFAULT_USER_EMAIL)
    )

    # Remove default user
    op.execute(
        sa.text(
            "DELETE FROM users WHERE email = :email"
        ).bindparams(email=DEFAULT_USER_EMAIL)
    )

"""Add vehicle_name and class_name to races table

Revision ID: 001
Revises: None
Create Date: 2025-01-01

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add vehicle_name column with default 'Unknown'
    op.add_column(
        'races',
        sa.Column('vehicle_name', sa.String(), nullable=False, server_default='Unknown')
    )

    # Add class_name column with default 'Unknown'
    op.add_column(
        'races',
        sa.Column('class_name', sa.String(), nullable=False, server_default='Unknown')
    )


def downgrade() -> None:
    op.drop_column('races', 'class_name')
    op.drop_column('races', 'vehicle_name')

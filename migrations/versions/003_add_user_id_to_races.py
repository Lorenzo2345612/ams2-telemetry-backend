"""Add user_id column to races table

Revision ID: 003
Revises: 002
Create Date: 2026-03-15

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'races',
        sa.Column('user_id', sa.String(), nullable=True),
    )
    op.create_foreign_key(
        'fk_races_user_id',
        'races',
        'users',
        ['user_id'],
        ['id'],
    )
    op.create_index('ix_races_user_id', 'races', ['user_id'])


def downgrade() -> None:
    op.drop_index('ix_races_user_id', table_name='races')
    op.drop_constraint('fk_races_user_id', 'races', type_='foreignkey')
    op.drop_column('races', 'user_id')

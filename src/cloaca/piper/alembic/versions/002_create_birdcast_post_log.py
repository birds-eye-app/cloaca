"""Create birdcast_post_log table

Revision ID: 002
Revises: 001
Create Date: 2026-04-12

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "birdcast_post_log",
        sa.Column("location", sa.Text, nullable=False),
        sa.Column("forecast_date", sa.Date, nullable=False),
        sa.Column(
            "posted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("location", "forecast_date"),
    )


def downgrade() -> None:
    op.drop_table("birdcast_post_log")

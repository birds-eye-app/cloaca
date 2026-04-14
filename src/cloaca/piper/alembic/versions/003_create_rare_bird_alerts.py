"""Create rare_bird_alerts table

Revision ID: 003
Revises: 002
Create Date: 2026-04-14

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "rare_bird_alerts",
        sa.Column("species_code", sa.Text, nullable=False),
        sa.Column("region_code", sa.Text, nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("aba_code", sa.Integer, nullable=False),
        sa.Column("obs_date", sa.Date, nullable=False),
        sa.Column("observer_name", sa.Text, nullable=False),
        sa.Column("sub_id", sa.Text, nullable=False),
        sa.Column("location_name", sa.Text, nullable=False),
        sa.Column(
            "alerted_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("species_code", "region_code", "obs_date"),
    )


def downgrade() -> None:
    op.drop_table("rare_bird_alerts")

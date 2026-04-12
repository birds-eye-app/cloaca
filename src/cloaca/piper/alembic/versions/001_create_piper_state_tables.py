"""Create piper state tables

Revision ID: 001
Revises:
Create Date: 2026-04-12

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "hotspot_year_species",
        sa.Column("hotspot_id", sa.Text, nullable=False),
        sa.Column("year", sa.Integer, nullable=False),
        sa.Column("species_code", sa.Text, nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, nullable=False),
        sa.Column("first_obs_date", sa.Date, nullable=False),
        sa.Column("observer_name", sa.Text, nullable=False),
        sa.Column("checklist_id", sa.Text, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("hotspot_id", "year", "species_code"),
    )

    op.create_table(
        "backfill_status",
        sa.Column("hotspot_id", sa.Text, nullable=False),
        sa.Column("year", sa.Integer, nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("species_count", sa.Integer, nullable=False),
        sa.PrimaryKeyConstraint("hotspot_id", "year"),
    )

    op.create_table(
        "hotspot_all_time_species",
        sa.Column("hotspot_id", sa.Text, nullable=False),
        sa.Column("species_code", sa.Text, nullable=False),
        sa.PrimaryKeyConstraint("hotspot_id", "species_code"),
    )

    op.create_table(
        "pending_provisional_lifers",
        sa.Column("hotspot_id", sa.Text, nullable=False),
        sa.Column("species_code", sa.Text, nullable=False),
        sa.Column("common_name", sa.Text, nullable=False),
        sa.Column("scientific_name", sa.Text, nullable=False),
        sa.Column("obs_date", sa.Date, nullable=False),
        sa.Column("observer_name", sa.Text, nullable=False),
        sa.Column("sub_id", sa.Text, nullable=False),
        sa.Column("lifer_type", sa.Text, nullable=False),
        sa.Column("year", sa.Integer, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.PrimaryKeyConstraint("hotspot_id", "species_code", "lifer_type"),
    )


def downgrade() -> None:
    op.drop_table("pending_provisional_lifers")
    op.drop_table("hotspot_all_time_species")
    op.drop_table("backfill_status")
    op.drop_table("hotspot_year_species")

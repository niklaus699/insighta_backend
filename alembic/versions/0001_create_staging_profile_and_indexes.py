"""create staging_profile table and recommended indexes

Revision ID: 0001_create_staging_profile_and_indexes
Revises: 
Create Date: 2026-05-04 00:00:00.000000
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '0001_create_staging_profile_and_indexes'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'staging_profile',
        sa.Column('id', sa.String(length=36), primary_key=True, nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('gender', sa.String(length=20), nullable=True),
        sa.Column('gender_probability', sa.Float(), nullable=True),
        sa.Column('sample_size', sa.Integer(), nullable=True),
        sa.Column('age', sa.Integer(), nullable=True),
        sa.Column('age_group', sa.String(length=20), nullable=True),
        sa.Column('country_id', sa.String(length=10), nullable=True),
        sa.Column('country_name', sa.String(length=100), nullable=True),
        sa.Column('country_probability', sa.Float(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
    )

    # Recommended indexes for `profile` table to cover hot query paths
    op.create_index('ix_profiles_country_gender_age', 'profile', ['country_id', 'gender', 'age'], unique=False)
    op.create_index('ix_profiles_gender_age_group', 'profile', ['gender', 'age_group'], unique=False)
    op.create_index('ix_profiles_created_at_id', 'profile', ['created_at', 'id'], unique=False)
    op.create_index('ix_profiles_country_gender_created_at_id', 'profile', ['country_id', 'gender', 'created_at', 'id'], unique=False)


def downgrade():
    op.drop_index('ix_profiles_country_gender_created_at_id', table_name='profile')
    op.drop_index('ix_profiles_created_at_id', table_name='profile')
    op.drop_index('ix_profiles_gender_age_group', table_name='profile')
    op.drop_index('ix_profiles_country_gender_age', table_name='profile')
    op.drop_table('staging_profile')

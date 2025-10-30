"""init tables

Revision ID: 0001_init
Revises: 
Create Date: 2025-10-30

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = '0001_init'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('import_jobs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('filename', sa.String(), nullable=True),
        sa.Column('user_id', sa.Integer(), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('meta', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'))
    )

    op.create_table('product_inputs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('import_job_id', sa.Integer(), sa.ForeignKey('import_jobs.id')),
        sa.Column('ean', sa.String(), nullable=True),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('purchase_price', sa.Numeric(), nullable=True),
        sa.Column('currency', sa.String(), nullable=True),
        sa.Column('normalized_price', sa.Numeric(), nullable=True),
        sa.Column('status', sa.String(), nullable=True),
        sa.Column('not_found', sa.Boolean(), nullable=False, server_default='false'),
        sa.Column('notes', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'))
    )

    op.create_table('allegro_cache',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('ean', sa.String(), unique=True, index=True),
        sa.Column('lowest_price', sa.Numeric(), nullable=True),
        sa.Column('sold_count', sa.Integer(), nullable=True),
        sa.Column('seller_info', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('source', sa.String(), nullable=True),
        sa.Column('fetched_at', sa.DateTime(timezone=True), nullable=True)
    )

    op.create_table('exports',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('import_job_id', sa.Integer(), nullable=True),
        sa.Column('filepath', sa.String(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'))
    )

def downgrade():
    op.drop_table('exports')
    op.drop_table('allegro_cache')
    op.drop_table('product_inputs')
    op.drop_table('import_jobs')

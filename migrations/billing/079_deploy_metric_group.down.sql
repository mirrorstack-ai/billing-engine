-- 079 (down): an enum value cannot be removed once added. Migration 080's down
-- moves every row off 'deploy' first, so the value is left unused; migration
-- 021's down drops the type with the column.
SELECT 1;

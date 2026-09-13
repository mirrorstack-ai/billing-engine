-- 081 down: nothing to undo. The guard values written up-migration are the
-- references the resolved headers already carried; clearing them would put
-- those apps back on every sweep's work list, which is the defect 081 fixes.
SELECT 1;

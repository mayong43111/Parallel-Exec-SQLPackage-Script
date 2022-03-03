## Parallel Exec SQLPackage Script

重要

分拆执行的时候并没有检查语句是否会导致数据丢失，并没做下面这样的检查

```
IF EXISTS (select top 1 1 from [xx].[table_name])
    RAISERROR (N'Rows were detected. The schema update is terminating because data loss might occur.', x, x) WITH NOWAIT

GO
```

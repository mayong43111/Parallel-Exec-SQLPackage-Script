## Parallel Exec SQLPackage Script

重要

这个版本增加了以下检查：

```
IF EXISTS (select top 1 1 from [xx].[table_name])
    RAISERROR (N'Rows were detected. The schema update is terminating because data loss might occur.', x, x) WITH NOWAIT

GO
```

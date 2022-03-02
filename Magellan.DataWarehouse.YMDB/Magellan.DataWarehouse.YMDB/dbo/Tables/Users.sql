﻿CREATE TABLE [dbo].[Users]
(
    UserID int NOT NULL,
    UserName nvarchar(255) NOT NULL,
    DisplayName nvarchar(255) NULL,
    CountryID int NOT NULL,
)
WITH
(
    DISTRIBUTION = HASH (UserID),
    CLUSTERED COLUMNSTORE INDEX
)
GO
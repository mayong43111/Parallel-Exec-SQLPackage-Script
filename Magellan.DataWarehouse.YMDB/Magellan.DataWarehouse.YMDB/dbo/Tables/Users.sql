CREATE TABLE [dbo].[Users]
(
    UserID int NOT NULL,
    UserName nvarchar(255) NOT NULL,
    DisplayName nvarchar(255) NULL,
    CountryID int NOT NULL,
)
WITH
(
    DISTRIBUTION = HASH (CountryID),
    CLUSTERED COLUMNSTORE INDEX
)
GO

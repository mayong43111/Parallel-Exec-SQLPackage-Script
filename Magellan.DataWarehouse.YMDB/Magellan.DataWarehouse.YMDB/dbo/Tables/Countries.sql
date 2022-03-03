CREATE TABLE [dbo].[Countries]
(
    CountryID int NOT NULL,
    DisplayName nvarchar(255) NOT NULL,
    ZoneID int NOT NULL,
)
WITH
(
    DISTRIBUTION = HASH (DisplayName),
    CLUSTERED COLUMNSTORE INDEX
)
GO

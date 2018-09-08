USE [KmsfOBExtracts]
GO

/****** Object:  Table [kmsf].[KmsfUkFinDataDumpProcLogDtl]    Script Date: 11/30/2017 9:54:15 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [kmsf].[KmsfUkFinDataDumpProcLogDtl](
	[LogEntryDtlId] [bigint] IDENTITY(1,1) NOT FOR REPLICATION NOT NULL,
	[LogEntryId] [bigint] NOT NULL,
	[StartDtime] [datetime] NULL,
	[EndDtime] [datetime] NULL,
	[RunDtime]  AS ((CONVERT([varchar],datediff(second,[StartDtime],isnull([EndDtime],sysdatetime()))/(((60)*(60))*(24)),(0))+':')+CONVERT([varchar],dateadd(second,datediff(second,[StartDtime],isnull([EndDtime],sysdatetime())),CONVERT([datetime2],'0001-01-01',(0))),(108))),
	[ProcName] [varchar](254) NULL,
	[ValueVariable] [varchar](254) NULL,
 CONSTRAINT [PK_KmsfUkFinDataDumpProcLogDtl] PRIMARY KEY CLUSTERED 
(
	[LogEntryDtlId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO



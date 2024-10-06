if object_id('syn.usp_ImportFileCustomerSeasonal', 'P') is not null
    drop procedure syn.usp_ImportFileCustomerSeasonal;

create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int not null
as
set nocount on

begin
	declare
		@ErrorMessage varchar(8000)
	
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)

	-- Проверка на корректность загрузки
	if not exists (
		select 1
		from syn.ImportFile as imf
		where imf.ID = @ID_Record
			and imf.FlagLoaded = cast(1 as bit)
	)
	begin
		set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'
	
		raiserror(@ErrorMessage, 3, 1)
		return
	end

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
		,system_user as MDT_ID_PrincipalCreatedBy
		,getdate() as MDT_DateCreate
	into #CustomerSeasonal
	from syn.SA_CustomerSeasonal cs
		inner join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		inner join dbo.Season as s on s.Name = cs.Season
		inner join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		inner join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	/*
		Определяем некорректные записи
		Добавляем причину, по которой запись считается некорректной
	*/
	select
		cs.*
		,case
			when c.ID is null
				then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null
				then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null
				then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null
				then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null
				then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null
				then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null
				then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
		left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
		left join dbo.Season as s on s.Name = cs.Season
		left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	where cc.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	merge syn.CustomerSeasonal as t
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = t.ID_dbo_Customer
		and s.ID_Season = t.ID_Season
		and s.DateBegin = t.DateBegin
	when matched and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set
			t.ID_CustomerSystemType = s.ID_CustomerSystemType
			,t.DateEnd = s.DateEnd
			,t.ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,t.FlagActive = s.FlagActive
		from syn.CustomerSeasonal as t
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Дата начала'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end

end

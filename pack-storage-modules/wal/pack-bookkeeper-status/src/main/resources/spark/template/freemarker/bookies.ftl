<html>
<head></head>
<body>


<table>
	<tr>
		<th>hostName</th>
		<th>port</th>
		<th>freeDiskSpace</th>
		<th>totalDiskSpace</th>
		<th>weight</th>
	</tr>
	<#list bookieInfoList as bookieInfo>
		<tr>
			<td>${bookieInfo.hostName}</td>
			<td>${bookieInfo.port}</td>
			<td>${bookieInfo.freeDiskSpace}</td>
			<td>${bookieInfo.totalDiskSpace}</td>
			<td>${bookieInfo.weight}</td>
		</tr>
	</#list>
</table>
</body>
</html>
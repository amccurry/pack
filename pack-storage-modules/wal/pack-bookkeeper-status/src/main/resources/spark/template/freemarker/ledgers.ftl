<html>
<head></head>
<body>


<table>
	<tr>
		<th>action</th>
		<th>ledgerId</th>
		<th>ackQuorumSize</th>
		<th>allEnsembles</th>
		<th>ctime</th>
		<th>customMetadata</th>
		<th>digestType</th>
		<th>ensembleSize</th>
		<th>lastEntryId</th>
		<th>length</th>
		<th>metadataFormatVersion</th>
		<th>state</th>
		<th>writeQuorumSize</th>
	</tr>
	<#list ledgerMetadataList as ledgerMetadata>
		<tr>
		    <td><form action="/bk/ledgers/delete/${ledgerMetadata.ledgerId}" method="post"><button type="submit">Delete</button></form></td>
			<td>${ledgerMetadata.ledgerId}</td>
			<td>${ledgerMetadata.ackQuorumSize}</td>
			<td>${ledgerMetadata.allEnsembles}</td>
			<td>${ledgerMetadata.ctime}</td>
			<td>${ledgerMetadata.customMetadata}</td>
			<td>${ledgerMetadata.digestType}</td>
			<td>${ledgerMetadata.ensembleSize}</td>
			<td>${ledgerMetadata.lastEntryId}</td>
			<td>${ledgerMetadata.length}</td>
			<td>${ledgerMetadata.metadataFormatVersion}</td>
			<td>${ledgerMetadata.state}</td>
			<td>${ledgerMetadata.writeQuorumSize}</td>
		</tr>
	</#list>
</table>
</body>
</html>
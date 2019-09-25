<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Pack ${name}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="/css/font.css" rel="stylesheet" type="text/css">
  <link href="/css/normalize.css" rel="stylesheet" type="text/css">
  <link href="/css/skeleton.css" rel="stylesheet" type="text/css">
</head>

<body>

<div class="container">
  <div class="row">
    <div class="two columns">
      <table class="u-full-width">
        <tr>
          <td>
            <#list menus>
              <#items as menu>
                <a class="button" href="${menu.link}">${menu.name}</a></br>
              </#items>
            </#list>
          </td>
        </tr>
      </table>
    </div>
    <div class="ten columns">
      <table class="u-full-width">
        <form method="POST">
          <tr>
            <#list actions>
              <th>
                <select name="action" onchange="this.form.submit()" style="color: #FFF; background-color: #33C3F0; border-color: #33C3F0;">
                  <option value="" selected >Action</option>
                  <#items as action>
                    <option value="${action}">${action}</option>
                  </#items>
                </select>
              </th>
            </#list>
            <#list headers as header>
              <th>${header}</th>
            </#list>
          </tr>
          <#list rows as row>
            <tr>
              <#if actions?size gt 0>
                <td><input type="checkbox" name="id" value="${row.id}"></td>
              </#if>
              <#list row.columns as column>
                <#if (column.value)??>
                  <td>${column.value}</td>
                <#else>
                  <td>&nbsp;</td>
                </#if>
              </#list>
            </tr>
          </#list>
        </form>
      </table>
    </div>
  </div>
</div>

</body>
</html>

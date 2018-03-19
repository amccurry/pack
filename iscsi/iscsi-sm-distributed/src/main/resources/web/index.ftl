<!DOCTYPE html>
<html>
  <head>
    <title>Pack</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="resources/css/bootstrap.min.css" rel="stylesheet" media="screen">
    <link href="resources/css/bs-docs.css" rel="stylesheet" media="screen">
  </head>
  <body>
    <div class="navbar navbar-inverse navbar-fixed-top">
      <div class="container">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="http://incubator.apache.org/blur">Pack</a>
        </div>
        <div class="collapse navbar-collapse">
          <ul class="nav navbar-nav">
            <li class="active"><a href="index.html">Cluster</a></li>
            <li><a href="volumes.html">Volumes</a></li>
            <li><a href="sessions.html">Sessions</a></li>
          </ul>
        </div>
      </div>
    </div>
    <div class="container bs-docs-container">
      <div class="row">
        <div class="col-md-3">
          <div class="bs-sidebar hidden-print affix" role="complementary">
            <ul class="nav bs-sidenav">
              <li><a href="#targets">Targets</a></li>
              <li><a href="#compactors">Compactors</a></li>
            </ul>
          </div>
        </div>
        <div class="col-md-9" role="main">
          <section>
            <div class="page-header">
              <h1 id="targets">Targets</h1>
            </div>
            <table class="table table-bordered table-striped table-condensed">
              <thead>
                <tr>
                  <th>Hostname</th>
                  <th>Address</th>
                  <th>Bind Address</th>
                </tr>
              </thead>
              <tbody>
<#list targets as target>
                <tr>
                  <td>${target.hostname}</td>
                  <td>${target.address}</td>
                  <td>${target.bindAddress}</td>
                </tr>
</#list>
              </tbody>
            </table>
        </section>
        <section>
          <div class="page-header">
            <h1 id="compactors">Compactors</h1>
          </div>
          <table class="table table-bordered table-striped table-condensed">
            <thead>
              <tr>
                <th>Hostname</th>
                <th>Address</th>
              </tr>
            </thead>
            <tbody>
<#list compactors as compactor>
                <tr>
                  <td>${compactor.hostname}</td>
                  <td>${compactor.address}</td>
                </tr>
</#list>
            </tbody>
          </table>
        </section>
      </div>
    </div>

    <script src="resources/js/jquery-2.0.3.min.js"></script>
    <script src="resources/js/bootstrap.min.js"></script>
    <script src="resources/js/respond.min.js"></script>
    <script src="resources/js/docs.js"></script>
  </body>
</html>

<!DOCTYPE html>
<html>
  <head>
    <title>Pack - Volumes</title>
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
            <li><a href="index.html">Cluster</a></li>
            <li class="active"><a href="volumes.html">Volumes</a></li>
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
              <li><a href="#volumes">Volumes</a></li>
            </ul>
          </div>
        </div>
        <div class="col-md-9" role="main">
          <section>
            <div class="page-header">
              <h1 id="volumes">Volumes</h1>
            </div>
            <table class="table table-bordered table-striped table-condensed">
              <thead>
                <tr>
                  <th>Volume</th>
                  <th>Size</th>
                  <th>IQN</th>
                  <th>Hdfs Path</th>
                  <th>Kafka Topic</th>
                </tr>
              </thead>
              <tbody>
<#list volumes as volume>
                <tr>
                  <td>${volume.name}</td>
                  <td>${volume.size}</td>
                  <td>${volume.iqn}</td>
                  <td>${volume.hdfsPath}</td>
                  <td>${volume.kafkaTopic}</td>
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

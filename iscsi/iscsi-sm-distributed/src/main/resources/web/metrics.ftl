<!DOCTYPE html>
<html>
  <head>
    <title>Pack - Metrics</title>
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
            <li><a href="volumes.html">Volumes</a></li>
            <li><a href="sessions.html">Sessions</a></li>
            <li class="active"><a href="metrics.html">Metrics</a></li>
          </ul>
        </div>
      </div>
    </div>
    <div class="container bs-docs-container">
      <div class="row">
        <div class="col-md-9" role="main">
          <section>
            <div class="page-header">
              <h1 id="volumes">Metrics</h1>
            </div>
            <table class="table table-bordered table-striped table-condensed">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>value</th>
                  <th>count</th>
                  <th>min</th>
                  <th>max</th>
                  <th>mean</th>
                  <th>stdDev</th>
                  <th>median</th>
                  <th>p75th</th>
                  <th>p95th</th>
                  <th>p98th</th>
                  <th>p99th</th>
                  <th>p999th</th>
                  <th>meanRate</th>
                  <th>oneMinuteRate</th>
                  <th>fiveMinuteRate</th>
                  <th>fifteenMinuteRate</th>
                </tr>
              </thead>
              <tbody>
<#list metrics as metric>
                <tr>
                  <td nowrap>${metric.name?if_exists}</td>
                  <td nowrap>${metric.type?if_exists}</td>
                  <td nowrap>${metric.value?if_exists}</td>
                  <td nowrap>${metric.count?if_exists}</td>
                  <td nowrap>${metric.min?if_exists}</td>
                  <td nowrap>${metric.max?if_exists}</td>
                  <td nowrap>${metric.mean?if_exists}</td>
                  <td nowrap>${metric.stdDev?if_exists}</td>
                  <td nowrap>${metric.median?if_exists}</td>
                  <td nowrap>${metric.p75th?if_exists}</td>
                  <td nowrap>${metric.p95th?if_exists}</td>
                  <td nowrap>${metric.p98th?if_exists}</td>
                  <td nowrap>${metric.p99th?if_exists}</td>
                  <td nowrap>${metric.p999th?if_exists}</td>
                  <td nowrap>${metric.meanRate?if_exists}</td>
                  <td nowrap>${metric.oneMinuteRate?if_exists}</td>
                  <td nowrap>${metric.fiveMinuteRate?if_exists}</td>
                  <td nowrap>${metric.fifteenMinuteRate?if_exists}</td>
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

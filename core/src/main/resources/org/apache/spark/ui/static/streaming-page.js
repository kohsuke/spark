/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


// timeFormat: StreamingPage.scala will generate a global "timeFormat" dictionary to store the time
// and its formatted string. Because we cannot specify a timezone in JavaScript, to make sure the
// server and client use the same timezone, we use the "timeFormat" dictionary to format all time
// values used in the graphs.

// A global margin left for all timeline graphs. It will be set in "registerTimeline". This will be
// used to align all timeline graphs.
var maxMarginLeftForTimeline = 0;

// The max X values for all histograms. It will be set in "registerHistogram".
var maxXForHistogram = 0;

var histogramBinCount = 10;
var yValueFormat = d3.format(",.2f");

var unitLabelYOffset = -10;

// Show a tooltip "text" for "node"
function showBootstrapTooltip(node, text) {
    $(node).tooltip({title: text, trigger: "manual", container: "body"});
    $(node).tooltip("show");
}

// Hide the tooltip for "node"
function hideBootstrapTooltip(node) {
    $(node).tooltip("destroy");
}

// Register a timeline graph. All timeline graphs should be register before calling any
// "drawTimeline" so that we can determine the max margin left for all timeline graphs.
function registerTimeline(minY, maxY) {
    var numOfChars = yValueFormat(maxY).length;
    // A least width for "maxY" in the graph
    var pxForMaxY = numOfChars * 8 + 10;
    // Make sure we have enough space to show the ticks in the y axis of timeline
    maxMarginLeftForTimeline = pxForMaxY > maxMarginLeftForTimeline? pxForMaxY : maxMarginLeftForTimeline;
}

// Register a histogram graph. All histogram graphs should be register before calling any
// "drawHistogram" so that we can determine the max X value for histograms.
function registerHistogram(values, minY, maxY) {
    var data = d3.layout.histogram().range([minY, maxY]).bins(histogramBinCount)(values);
    // d.x is the y values while d.y is the x values
    var maxX = d3.max(data, function(d) { return d.y; });
    maxXForHistogram = maxX > maxXForHistogram ? maxX : maxXForHistogram;
}

// Draw a line between (x1, y1) and (x2, y2)
function drawLine(svg, xFunc, yFunc, x1, y1, x2, y2) {
    var line = d3.svg.line()
        .x(function(d) { return xFunc(d.x); })
        .y(function(d) { return yFunc(d.y); });
    var data = [{x: x1, y: y1}, {x: x2, y: y2}];
    svg.append("path")
        .datum(data)
        .style("stroke-dasharray", ("6, 6"))
        .style("stroke", "lightblue")
        .attr("class", "line")
        .attr("d", line);
}

/**
 * @param id the `id` used in the html `div` tag
 * @param data the data for the timeline graph
 * @param minX the min value of X axis
 * @param maxX the max value of X axis
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if "batchInterval" is specified, we will draw a line for "batchInterval" in the graph
 */
function drawTimeline(id, data, minX, maxX, minY, maxY, unitY, batchInterval) {
    // Hide the right border of "<td>". We cannot use "css" directly, or "sorttable.js" will override them.
    d3.select(d3.select(id).node().parentNode)
        .style("padding", "8px 0 8px 8px")
        .style("border-right", "0px solid white");

    var margin = {top: 20, right: 27, bottom: 30, left: maxMarginLeftForTimeline};
    var width = 500 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    var x = d3.scale.linear().domain([minX, maxX]).range([0, width]);
    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);

    var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(function(d) {
        var formattedDate = timeFormat[d];
        var dotIndex = formattedDate.indexOf('.');
        if (dotIndex >= 0) {
            // Remove milliseconds
            return formattedDate.substring(0, dotIndex);
        } else {
            return formattedDate;
        }
    });
    var formatYValue = d3.format(",.2f");
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(5).tickFormat(formatYValue);

    var line = d3.svg.line()
        .x(function(d) { return x(d.x); })
        .y(function(d) { return y(d.y); });

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    // Only show the first and last time in the graph
    xAxis.tickValues(x.domain());

    svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis)
        .append("text")
            .attr("transform", "translate(0," + unitLabelYOffset + ")")
            .text(unitY);


    if (batchInterval && batchInterval <= maxY) {
        drawLine(svg, x, y, minX, batchInterval, maxX, batchInterval);
    }

    svg.append("path")
        .datum(data)
        .attr("class", "line")
        .attr("d", line);

    // If the user click one point in the graphs, jump to the batch row and highlight it. And
    // recovery the batch row after 3 seconds if necessary.
    // We need to remember the last clicked batch so that we can recovery it.
    var lastClickedBatch = null;
    var lastTimeout = null;

    function isFailedBatch(batchTime) {
        return $("#batch-" + batchTime).attr("isFailed") == "true";
    }

    // Add points to the line. However, we make it invisible at first. But when the user moves mouse
    // over a point, it will be displayed with its detail.
    svg.selectAll(".point")
        .data(data)
        .enter().append("circle")
            .attr("stroke", function(d) { return isFailedBatch(d.x) ? "red" : "white";}) // white and opacity = 0 make it invisible
            .attr("fill", function(d) { return isFailedBatch(d.x) ? "red" : "white";})
            .attr("opacity", function(d) { return isFailedBatch(d.x) ? "1" : "0";})
            .style("cursor", "pointer")
            .attr("cx", function(d) { return x(d.x); })
            .attr("cy", function(d) { return y(d.y); })
            .attr("r", function(d) { return isFailedBatch(d.x) ? "2" : "3";})
            .on('mouseover', function(d) {
                var tip = formatYValue(d.y) + " " + unitY + " at " + timeFormat[d.x];
                showBootstrapTooltip(d3.select(this).node(), tip);
                // show the point
                d3.select(this)
                    .attr("stroke", function(d) { return isFailedBatch(d.x) ? "red" : "steelblue";})
                    .attr("fill", function(d) { return isFailedBatch(d.x) ? "red" : "steelblue";})
                    .attr("opacity", "1")
                    .attr("r", "3");
            })
            .on('mouseout',  function() {
                hideBootstrapTooltip(d3.select(this).node());
                // hide the point
                d3.select(this)
                    .attr("stroke", function(d) { return isFailedBatch(d.x) ? "red" : "white";})
                    .attr("fill", function(d) { return isFailedBatch(d.x) ? "red" : "white";})
                    .attr("opacity", function(d) { return isFailedBatch(d.x) ? "1" : "0";})
                    .attr("r", function(d) { return isFailedBatch(d.x) ? "2" : "3";});
            })
            .on("click", function(d) {
                if (lastTimeout != null) {
                    window.clearTimeout(lastTimeout);
                }
                if (lastClickedBatch != null) {
                    clearBatchRow(lastClickedBatch);
                    lastClickedBatch = null;
                }
                lastClickedBatch = d.x;
                highlightBatchRow(lastClickedBatch);
                lastTimeout = window.setTimeout(function () {
                    lastTimeout = null;
                    if (lastClickedBatch != null) {
                        clearBatchRow(lastClickedBatch);
                        lastClickedBatch = null;
                    }
                }, 3000); // Clean up after 3 seconds

                var batchSelector = $("#batch-" + d.x);
                var topOffset = batchSelector.offset().top - 15;
                if (topOffset < 0) {
                    topOffset = 0;
                }
                $('html,body').animate({scrollTop: topOffset}, 200);
            });
}

/**
 * @param id the `id` used in the html `div` tag
 * @param values the data for the histogram graph
 * @param minY the min value of Y axis
 * @param maxY the max value of Y axis
 * @param unitY the unit of Y axis
 * @param batchInterval if "batchInterval" is specified, we will draw a line for "batchInterval" in the graph
 */
function drawHistogram(id, values, minY, maxY, unitY, batchInterval) {
    // Hide the left border of "<td>". We cannot use "css" directly, or "sorttable.js" will override them.
    d3.select(d3.select(id).node().parentNode)
        .style("padding", "8px 8px 8px 0")
        .style("border-left", "0px solid white");

    var margin = {top: 20, right: 30, bottom: 30, left: 10};
    var width = 350 - margin.left - margin.right;
    var height = 150 - margin.top - margin.bottom;

    var x = d3.scale.linear().domain([0, maxXForHistogram]).range([0, width - 50]);
    var y = d3.scale.linear().domain([minY, maxY]).range([height, 0]);

    var xAxis = d3.svg.axis().scale(x).orient("top").ticks(5);
    var yAxis = d3.svg.axis().scale(y).orient("left").ticks(0).tickFormat(function(d) { return ""; });

    var data = d3.layout.histogram().range([minY, maxY]).bins(histogramBinCount)(values);

    var svg = d3.select(id).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
            .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    if (batchInterval && batchInterval <= maxY) {
        drawLine(svg, x, y, 0, batchInterval, maxXForHistogram, batchInterval);
    }

    svg.append("g")
        .attr("class", "x axis")
        .call(xAxis)
        .append("text")
            .attr("transform", "translate(" + (margin.left + width - 45) + ", " + unitLabelYOffset + ")")
            .text("#batches");

    svg.append("g")
        .attr("class", "y axis")
        .call(yAxis);

    svg.selectAll(".bar")
        .data(data)
        .enter()
        .append("g")
            .attr("transform", function(d) { return "translate(0," + (y(d.x) - height + y(d.dx))  + ")";})
            .attr("class", "bar").append("rect")
            .attr("width", function(d) { return x(d.y); })
            .attr("height", function(d) { return height - y(d.dx); })
            .on('mouseover', function(d) {
                var percent = yValueFormat(d.y * 100.0 / values.length) + "%";
                var tip = d.y + " batches (" + percent + ") between " + yValueFormat(d.x) + " and " + yValueFormat(d.x + d.dx) + " " + unitY;
                showBootstrapTooltip(d3.select(this).node(), tip);
            })
            .on('mouseout',  function() {
                hideBootstrapTooltip(d3.select(this).node());
            });

    if (batchInterval && batchInterval <= maxY) {
        // Add the "stable" text to the graph below the batch interval line.
        var stableXOffset = x(maxXForHistogram) - 20;
        var stableYOffset = y(batchInterval) + 15;
        svg.append("text")
            .style("fill", "lightblue")
            .attr("class", "stable-text")
            .attr("text-anchor", "middle")
            .attr("transform", "translate(" + stableXOffset + "," + stableYOffset + ")")
            .text("stable")
            .on('mouseover', function(d) {
              var tip = "Processing Time <= Batch Interval (" + yValueFormat(batchInterval) +" " + unitY +")";
              showBootstrapTooltip(d3.select(this).node(), tip);
            })
            .on('mouseout',  function() {
              hideBootstrapTooltip(d3.select(this).node());
            });
    }
}

function drawAreaStack(id, labels, values, minX, maxX, minY, maxY) {
    d3.select(d3.select(id).node().parentNode)
        .style("padding", "8px 0 8px 8px")
        .style("border-right", "0px solid white");

    // Setup svg using Bostock's margin convention
    var margin = {top: 20, right: 27, bottom: 30, left: maxMarginLeftForTimeline};
    var width = 850 - margin.left - margin.right;
    var height = 300 - margin.top - margin.bottom;

    var svg = d3.select(id)
      .append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var data = values;

    var parse = d3.time.format("%H:%M:%S.%L").parse;

    // Transpose the data into layers
    var dataset = d3.layout.stack()(labels.map(function(fruit) {
      return data.map(function(d) {
        return {_x: d.x, x: parse(d.x), y: +d[fruit]};
      });
    }));


    // Set x, y and colors
    var x = d3.scale.ordinal()
      .domain(dataset[0].map(function(d) { return d.x; }))
      .rangeRoundBands([10, width-10], 0.02);

    var y = d3.scale.linear()
      .domain([0, d3.max(dataset, function(d) {  return d3.max(d, function(d) { return d.y0 + d.y; });  })])
      .range([height, 0]);

    var colors = ["#0088cc", "#AFEEEE", "#FF8C00", "#3CB371", "#FF0000"];

    // Define and draw axes
    var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left")
      .ticks(7)
      .tickFormat( function(d) { return d } );

    var xAxis = d3.svg.axis()
      .scale(x)
      .orient("bottom")
      .tickFormat(d3.time.format("%H:%M:%S.%L"));

    // Only show the first and last time in the graph
    var xline = []
    xline.push(x.domain()[0])
    xline.push(x.domain()[x.domain().length - 1])
    xAxis.tickValues(xline);

    svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
      .append("text")
          .attr("transform", "translate(0," + unitLabelYOffset + ")")
          .text("ms");

    svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

    // Create groups for each series, rects for each segment
    var groups = svg.selectAll("g.cost")
      .data(dataset)
      .enter().append("g")
      .attr("class", "cost")
      .style("fill", function(d, i) { return colors[i]; });

    var rect = groups.selectAll("rect")
      .data(function(d) { return d; })
      .enter()
      .append("rect")
      .attr("x", function(d) { return x(d.x); })
      .attr("y", function(d) { return y(d.y0 + d.y); })
      .attr("height", function(d) { return y(d.y0) - y(d.y0 + d.y); })
      .attr("width", x.rangeBand())
      .on('mouseover', function(d) {
        var tip = '';
        var idx = 0;
        var _values = timeToValues[d._x]
        _values.forEach(function (k) {
          tip += labels[idx] + ': ' + k + '   ';
          idx += 1;
        });
        tip += " at " + d._x
        showBootstrapTooltip(d3.select(this).node(), tip);
      })
      .on('mouseout',  function() {
        hideBootstrapTooltip(d3.select(this).node());
      })
      .on("mousemove", function(d) {
        var xPosition = d3.mouse(this)[0] - 15;
        var yPosition = d3.mouse(this)[1] - 25;
        tooltip.attr("transform", "translate(" + xPosition + "," + yPosition + ")");
        tooltip.select("text").text(d.y);
      });


    // Draw legend
    var legend = svg.selectAll(".legend")
      .data(colors)
      .enter().append("g")
      .attr("class", "legend")
      .attr("transform", function(d, i) { return "translate(30," + i * 19 + ")"; });

    legend.append("rect")
      .attr("x", width - 100)
      .attr("width", 18)
      .attr("height", 18)
      .style("fill", function(d, i) {return colors.slice().reverse()[i];});

    legend.append("text")
      .attr("x", width - 80)
      .attr("y", 9)
      .attr("dy", ".35em")
      .style("text-anchor", "start")
      .text(function(d, i) {
        var len = labels.length
        return labels[len - 1 -i]
      });


    // Prep the tooltip bits, initial display is hidden
    var tooltip = svg.append("g")
      .attr("class", "tooltip")
      .style("display", "none");

    tooltip.append("rect")
      .attr("width", 30)
      .attr("height", 20)
      .attr("fill", "white")
      .style("opacity", 0.5);

    tooltip.append("text")
      .attr("x", 15)
      .attr("dy", "1.2em")
      .style("text-anchor", "middle")
      .attr("font-size", "12px")
      .attr("font-weight", "bold");
}

$(function() {
    var status = window.localStorage && window.localStorage.getItem("show-streams-detail") == "true";

    $("span.expand-input-rate").click(function() {
        status = !status;
        $("#inputs-table").toggle('collapsed');
        // Toggle the class of the arrow between open and closed
        $(this).find('.expand-input-rate-arrow').toggleClass('arrow-open').toggleClass('arrow-closed');
        if (window.localStorage) {
            window.localStorage.setItem("show-streams-detail", "" + status);
        }
    });

    if (status) {
        $("#inputs-table").toggle('collapsed');
        // Toggle the class of the arrow between open and closed
        $(this).find('.expand-input-rate-arrow').toggleClass('arrow-open').toggleClass('arrow-closed');
    }
});

function highlightBatchRow(batch) {
    $("#batch-" + batch).parent().addClass("batch-table-cell-highlight");
}

function clearBatchRow(batch) {
    $("#batch-" + batch).parent().removeClass("batch-table-cell-highlight");
}
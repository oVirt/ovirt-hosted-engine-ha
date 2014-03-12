from gen_gv import get_rules
import ovirt_hosted_engine_ha.agent.states
import ovirt_hosted_engine_ha.agent.state_machine
import json

if __name__ == "__main__":

    print "<!--"

    rules = get_rules([ovirt_hosted_engine_ha.agent.state_machine,
                       ovirt_hosted_engine_ha.agent.states])

    states = set(rules.keys())
    for k, v in rules.iteritems():
        states.union(v.keys())

    states = list(states)
    table = []
    for start in states:
        row = []
        for end in states:
            row.append(1 if end in rules[start] else 0)
        table.append(row)

    data = json.dumps({
        "packageNames": states,
        "matrix": table
    })

    page = """
//-->
<html>
<head>
<title>Hosted Engine HA agent state machine transitions</title>
<script src="d3.v3.min.js" charset="utf-8"></script>
<script src="d3.dependencyWheel.js"></script>
</head>
<body>
<div id='graph'>
</div>
<script language='javascript'>
var data = %(data)s;
var chart = d3.chart.dependencyWheel()
  .width(700)    // also used for height, since the wheel is in a a square
  .margin(150)   // used to display package names
  .padding(.02); // separating groups in the wheel
d3.select('#graph')
  .datum(data)
  .call(chart);
</script>
</body>
</html>""" % {"data": data}

    print page

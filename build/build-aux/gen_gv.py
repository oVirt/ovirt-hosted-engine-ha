__author__ = 'msivak'
import ovirt_hosted_engine_ha.agent.states
import ovirt_hosted_engine_ha.agent.state_machine
import re
import inspect

TRANSITION_RULE = re.compile(r":transition( (?P<dest>[^ -]*))?:(?P<doc>.*)")

def process_cls(cls, rules, copy_rules):
    cls_name = cls.__name__
    cls_rules = {}
    if cls.__doc__ is None:
        return

    for raw in cls.__doc__.split("\n"):
        line = raw.strip()
        if not line.startswith(":transition"):
            continue
        elif line.startswith(":transitions_from "):
            copy_from = line.split(" ")[1][:-1]
            copy_rules.append((copy_from, cls_name))
            print "// copy rule %s -> %s" % (copy_from, cls_name)
        else:
            m = TRANSITION_RULE.match(line)
            if m is None:
                print "// no match", line
                continue
            dest = m.group("dest")
            if dest is None:
                dest = cls_name
            cls_rules[dest] = m.group("doc").strip()

    if cls_rules:
        rules[cls_name] = cls_rules

def get_rules(modules):
    rules = {}
    copy_rules = []
    for module in modules:
        print "// ", module
        clss = inspect.getmembers(module, inspect.isclass)
        for _name, cls in clss:
            print "// ", cls
            if not hasattr(cls, "consume"):
                print "// skip"
                continue
            process_cls(cls, rules, copy_rules)
    for src, dest in copy_rules:
        rules[dest].update(rules[src])
    return rules

if __name__ == "__main__":
    rules = get_rules([ovirt_hosted_engine_ha.agent.state_machine,
                       ovirt_hosted_engine_ha.agent.states])
    print """/**
 * This file contains the source for a graphviz FSM diagram of the HA agent
 * state machine.  To create an image, fsm.png in this case, run the following:
 *
 *   dot agent-fsm.gv -Tpng -o fsm.png
 *
 * A copy of the latest diagram should be available at:
 *
 *   http://www.ovirt.org/Features/Self_Hosted_Engine#Agent_State_Diagram
 */

digraph finite_state_machine {
ranksep = 0.5;
node [shape = doublecircle]; StartState;
node [shape = circle];""", " ".join(s for s in rules.iterkeys() if s != "StartState"), ";"

    for src, v in rules.iteritems():
        for dest, doc in v.iteritems():
            print '%s -> %s [ label = "%s" ];' % (src, dest, doc)

    print "}"

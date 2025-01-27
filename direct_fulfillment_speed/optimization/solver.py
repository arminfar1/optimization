from typing import List

try:
    import amazon_xpress_license
    import xpress
except Exception:
    import xpress


class Solver:
    def __init__(self, modelName, typeStr="MINIMIZE"):
        xpress.beginlicensing()
        oem_num, oem_str = xpress.license(0, "")
        oem_lic = 63112059 - (oem_num * oem_num) // 19
        oem_num, oem_str = xpress.license(oem_lic, oem_str)
        xpress.init()
        xpress.endlicensing()
        self.model = xpress.problem(name=modelName)
        self.probType = self.setProbType(typeStr)

    def setProbType(self, typeStr):
        if typeStr == "MINIMIZE":
            return xpress.minimize
        elif typeStr == "MAXIMIZE":
            return xpress.maximize

    def setVarType(self, typeStr):
        if typeStr == "BINARY":
            return xpress.binary
        elif typeStr == "INTEGER":
            return xpress.integer
        elif typeStr == "CONTINUOUS":
            return xpress.continuous

    def setCstType(self, typeStr):
        if typeStr == "EQUAL":
            return xpress.eq
        elif typeStr == "GREATER_EQUAL":
            return xpress.geq
        elif typeStr == "LESS_EQUAL":
            return xpress.leq

    def setlogfile(self, optionStr):
        self.model.setlogfile(optionStr)

    def setOutputFlag(self, optionStr):
        self.model.setControl("outputlog", optionStr)

    def setMaxTime(self, optionStr):
        self.model.setControl("maxtime", optionStr)

    def setMIPGap(self, optionStr):
        self.model.setControl("miprelstop", optionStr)

    def setThreads(self, optionStr):
        self.model.setControl("threads", optionStr)

    def setControl(self, controlStr, optionStr):
        self.model.setControl(controlStr, optionStr)

    def getControl(self, controlStr):
        return self.model.getControl(controlStr)

    def addObjective(self, obj):
        self.model.setObjective(obj, sense=self.probType)

    def getAttrib(self, attributeStr):
        return self.model.getAttrib(attributeStr)

    def addVariable(self, var):
        self.model.addVariable(var)

    def delVariable(self, varList):
        self.model.delVariable(varList)

    def chgObj(self, var, obj):
        self.model.chgobj([var], [obj])

    def addConstraint(self, nameStr, cstStr, lhs, rhs):
        cst = xpress.constraint(body=lhs, sense=self.setCstType(cstStr), rhs=rhs, name=nameStr)
        self.model.addConstraint(cst)
        return cst

    def getIndex(self, var):
        return self.model.getIndex(var)

    def delConstraint(self, cstList):
        self.model.delConstraint(cstList)

    def sum(self, express):
        return xpress.Sum(express)

    def chgRHS(self, cst, rhs):
        self.model.chgrhs([cst], [rhs])

    def write(self, fileNameStr):
        self.model.write(fileNameStr, "lP")

    def mip_optimize(self, flags=""):
        self.model.mipoptimize(flags)

    def lp_optimize(self, flags=""):
        self.model.lpoptimize(flags)

    def getAttribute(self, attrStr):
        if attrStr == "COLS":
            return self.model.attributes.cols
        elif attrStr == "ROWS":
            return self.model.attributes.rows
        elif attrStr == "MIPOBJVAL":
            return self.model.attributes.mipobjval
        elif attrStr == "BESTBOUND":
            return self.model.attributes.bestbound

    def getSolution(self, var=None):
        if not var:
            return self.model.getSolution()
        else:
            return self.model.getSolution(var)

    def getMIPSolution(self):
        tmp_sol: List = []
        self.model.getmipsol(tmp_sol)
        return tmp_sol

    def getVariable(self, index, first, last):
        if index or first or last:
            return self.model.getVariable(index, first, last)
        return self.model.getVariable()

    def clean(self):
        self.model.delVariable(self.model.getVariable())
        self.model.delConstraint(self.model.getConstraint())

    def reset(self):
        self.model.reset()

    def getObjectiveValue(self):
        return self.model.getObjVal()

    def loadMIPSol(self, sol):
        return self.model.loadmipsol(sol)

    def changeBounds(self, mindex, qbtype, bnd):
        self.model.chgbounds(mindex, qbtype, bnd)

    def getStatus(self):
        return self.model.getProbStatusString()

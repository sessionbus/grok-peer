// SPDX-License-Identifier: MIT

package grok

const sessionbusNativeTool = "sessionbus__sessionbus"
const sessionbusNativeRule = "MCPTool(" + sessionbusNativeTool + ")"

func sessionbusLeaderPolicy() []string {
	return []string{"--allow", sessionbusNativeRule}
}

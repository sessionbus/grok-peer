// SPDX-License-Identifier: MIT

package grok

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

const grokConfigFile = "config.toml"
const grokConfigLockFile = ".config-init.lock"

type permissionRepresentation uint8

const (
	permissionAbsent permissionRepresentation = iota
	permissionCompact
	permissionStructured
)

type permissionConfig struct {
	root           map[string]any
	permission     map[string]any
	representation permissionRepresentation
	granted        bool
}

// ensureSessionbusPermission maintains the one exact global Grok permission
// rule which makes the managed Sessionbus tool prompt-free. Native Grok reads
// this file for every session spawn. It gives compact deny/allow/ask arrays
// precedence over permission.rules and evaluates deny > ask > allow, so this
// function preserves the active representation and never removes a rule.
func ensureSessionbusPermission(environment []string, cwd string) error {
	home, err := resolvedGrokHome(environment, cwd)
	if err != nil {
		return err
	}
	return ensureSessionbusPermissionAt(home)
}

func resolvedGrokHome(environment []string, cwd string) (string, error) {
	if home := lastEnvironmentValue(environment, "GROK_HOME"); home != "" {
		if filepath.IsAbs(home) {
			return home, nil
		}
		return filepath.Join(cwd, home), nil
	}
	home := lastEnvironmentValue(environment, "HOME")
	if home == "" {
		current, err := user.Current()
		if err != nil || current.HomeDir == "" {
			return "", errors.New("resolve Grok home: neither GROK_HOME, HOME, nor the current user home is available")
		}
		home = current.HomeDir
	}
	if !filepath.IsAbs(home) {
		home = filepath.Join(cwd, home)
	}
	return filepath.Join(home, ".grok"), nil
}

func lastEnvironmentValue(environment []string, name string) string {
	for index := len(environment) - 1; index >= 0; index-- {
		if key, value, found := strings.Cut(environment[index], "="); found && key == name {
			return value
		}
	}
	return ""
}

func ensureSessionbusPermissionAt(home string) error {
	_, err := ensureSessionbusConfigAt(home)
	return err
}

// ensureManagedConfig is the interactive launch's config step: the permission rule as
// for every managed launch and, from that same read, the helper's registration. Only
// the installer writes the registration, so a config which decodes without the
// mcp_servers.sessionbus table refuses the launch.
func ensureManagedConfig(environment []string, cwd string) error {
	home, err := resolvedGrokHome(environment, cwd)
	if err != nil {
		return err
	}
	parsed, err := ensureSessionbusConfigAt(home)
	if err != nil {
		return err
	}
	servers, _ := parsed.root["mcp_servers"].(map[string]any)
	if _, registered := servers["sessionbus"].(map[string]any); !registered {
		return fmt.Errorf("Grok config %s has no mcp_servers.sessionbus entry; run the grok-peer installer to register the Sessionbus helper", filepath.Join(home, grokConfigFile))
	}
	return nil
}

// ensureSessionbusConfigAt returns the config as its one read decoded it. Adding the
// permission rule changes no other field.
func ensureSessionbusConfigAt(home string) (permissionConfig, error) {
	if err := os.MkdirAll(home, 0700); err != nil {
		return permissionConfig{}, fmt.Errorf("create Grok home %s: %w", home, err)
	}
	lock, err := acquireGrokConfigLock(home)
	if err != nil {
		return permissionConfig{}, err
	}
	defer func() {
		_ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
		_ = lock.Close()
	}()

	slot := filepath.Join(home, grokConfigFile)
	dest, err := bindGrokConfigDestination(slot)
	if err != nil {
		return permissionConfig{}, fmt.Errorf("bind Grok config %s: %w", slot, err)
	}
	content, err := readGrokConfig(dest)
	if err != nil {
		return permissionConfig{}, fmt.Errorf("read Grok config %s: %w", slot, err)
	}
	parsed, err := parsePermissionConfig(content)
	if err != nil {
		return permissionConfig{}, fmt.Errorf("refusing to update Grok config %s: %w", slot, err)
	}
	if parsed.granted {
		return parsed, nil
	}
	candidate, expected, err := addSessionbusPermission(content, parsed)
	if err != nil {
		return permissionConfig{}, fmt.Errorf("refusing to update Grok config %s: %w", slot, err)
	}
	verified, err := parsePermissionConfig(candidate)
	if err != nil {
		return permissionConfig{}, fmt.Errorf("refusing to update Grok config %s: generated TOML is invalid: %w", slot, err)
	}
	if !verified.granted || !reflect.DeepEqual(verified.root, expected) {
		return permissionConfig{}, fmt.Errorf("refusing to update Grok config %s: candidate changes fields beyond the exact Sessionbus allow rule", slot)
	}
	if err := writeGrokConfigAtomically(slot, dest, content, candidate); err != nil {
		return permissionConfig{}, fmt.Errorf("update Grok config %s: %w", slot, err)
	}
	return parsed, nil
}

func acquireGrokConfigLock(home string) (*os.File, error) {
	path := filepath.Join(home, grokConfigLockFile)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open Grok config lock %s: %w", path, err)
	}
	for range 50 {
		err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return file, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
			_ = file.Close()
			return nil, fmt.Errorf("lock Grok config %s: %w", path, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = file.Close()
	return nil, fmt.Errorf("timed out waiting for Grok config lock %s after 1s", path)
}

func parsePermissionConfig(content []byte) (permissionConfig, error) {
	root := map[string]any{}
	if strings.TrimSpace(string(content)) != "" {
		if _, err := toml.Decode(string(content), &root); err != nil {
			return permissionConfig{}, sanitizedTOMLError(err)
		}
	}
	result := permissionConfig{root: root, representation: permissionAbsent}
	raw, exists := root["permission"]
	if !exists {
		return result, nil
	}
	permission, ok := raw.(map[string]any)
	if !ok {
		return permissionConfig{}, errors.New("permission must be a table")
	}
	result.permission = permission
	result.representation = permissionStructured
	for _, key := range []string{"deny", "allow", "ask"} {
		if value, exists := permission[key]; exists {
			if _, ok := value.([]any); ok {
				result.representation = permissionCompact
			}
		}
	}
	if result.representation == permissionCompact {
		if value, exists := permission["allow"]; exists {
			items, ok := value.([]any)
			if !ok {
				return permissionConfig{}, errors.New("permission.allow must be an array while compact permission rules are active")
			}
			for _, item := range items {
				if rule, ok := item.(string); ok && strings.TrimSpace(rule) == sessionbusNativeRule {
					result.granted = true
				}
			}
		}
		return result, nil
	}

	if value, exists := permission["rules"]; exists {
		rules, ok := permissionRules(value)
		if !ok {
			return permissionConfig{}, errors.New("permission.rules must be an array of tables")
		}
		for _, rule := range rules {
			mode, hasMode := rule["pattern_mode"]
			if rule["action"] == "allow" && rule["tool"] == "mcp" && rule["pattern"] == sessionbusNativeTool && (!hasMode || mode == "glob") {
				result.granted = true
			}
		}
	}
	return result, nil
}

func addSessionbusPermission(content []byte, parsed permissionConfig) ([]byte, map[string]any, error) {
	expected, err := cloneTOMLRoot(content)
	if err != nil {
		return nil, nil, err
	}
	newline := tomlNewline(content)
	var candidate []byte
	switch parsed.representation {
	case permissionAbsent:
		candidate = appendTOMLBlock(content, "[permission]"+newline+"allow = [\""+sessionbusNativeRule+"\"]"+newline, newline)
		expected["permission"] = map[string]any{"allow": []any{sessionbusNativeRule}}
	case permissionStructured:
		if parsed.permission == nil {
			return nil, nil, errors.New("permission table is unavailable")
		}
		permission := expected["permission"].(map[string]any)
		newRule := map[string]any{"action": "allow", "tool": "mcp", "pattern": sessionbusNativeTool}
		if raw, exists := permission["rules"]; exists {
			switch rules := raw.(type) {
			case []any:
				source, err := scanPermissionSource(content)
				if err != nil {
					return nil, nil, err
				}
				if source.rulesOpen < 0 || source.rulesClose < 0 {
					return nil, nil, errors.New("permission.rules uses a TOML shape the bounded updater cannot edit losslessly")
				}
				addition := "{ action = \"allow\", tool = \"mcp\", pattern = \"" + sessionbusNativeTool + "\" }"
				if len(rules) > 0 {
					if source.rulesTrailingComma {
						addition = " " + addition
					} else {
						addition = ", " + addition
					}
				}
				permission["rules"] = append(rules, newRule)
				candidate = insertBytes(content, source.rulesClose, addition)
			case []map[string]any:
				candidate = appendTOMLBlock(content, "[[permission.rules]]"+newline+"action = \"allow\""+newline+"tool = \"mcp\""+newline+"pattern = \""+sessionbusNativeTool+"\""+newline, newline)
				permission["rules"] = append(rules, newRule)
			default:
				return nil, nil, errors.New("permission.rules array style does not match its source")
			}
		} else {
			candidate = appendTOMLBlock(content, "[[permission.rules]]"+newline+"action = \"allow\""+newline+"tool = \"mcp\""+newline+"pattern = \""+sessionbusNativeTool+"\""+newline, newline)
			permission["rules"] = []map[string]any{newRule}
		}
	case permissionCompact:
		source, err := scanPermissionSource(content)
		if err != nil {
			return nil, nil, err
		}
		permission := expected["permission"].(map[string]any)
		if raw, exists := permission["allow"]; exists {
			items := raw.([]any)
			if source.allowOpen < 0 || source.allowClose < 0 {
				return nil, nil, errors.New("permission.allow uses a TOML shape the bounded updater cannot edit losslessly")
			}
			addition := "\"" + sessionbusNativeRule + "\""
			if len(items) > 0 {
				if source.allowTrailingComma {
					addition = " " + addition
				} else {
					addition = ", " + addition
				}
			}
			candidate = insertBytes(content, source.allowClose, addition)
			permission["allow"] = append(items, sessionbusNativeRule)
		} else {
			line := "allow = [\"" + sessionbusNativeRule + "\"]" + newline
			if source.permissionHeaderAfter >= 0 {
				addition := line
				if source.permissionHeaderAfter == len(content) && !endsTOMLLine(content) {
					addition = newline + line
				}
				candidate = insertBytes(content, source.permissionHeaderAfter, addition)
			} else {
				// A top-level dotted permission table may have no [permission]
				// header. Prepending the sibling dotted key preserves all bytes;
				// the post-decode equality check rejects inline-table conflicts.
				candidate = append([]byte("permission.allow = [\""+sessionbusNativeRule+"\"]"+newline), content...)
			}
			permission["allow"] = []any{sessionbusNativeRule}
		}
	default:
		return nil, nil, errors.New("unknown permission representation")
	}
	return candidate, expected, nil
}

func cloneTOMLRoot(content []byte) (map[string]any, error) {
	root := map[string]any{}
	if strings.TrimSpace(string(content)) == "" {
		return root, nil
	}
	_, err := toml.Decode(string(content), &root)
	if err != nil {
		return nil, sanitizedTOMLError(err)
	}
	return root, nil
}

func sanitizedTOMLError(err error) error {
	var parse toml.ParseError
	if errors.As(err, &parse) {
		return fmt.Errorf("invalid TOML at line %d, column %d", parse.Position.Line, parse.Position.Col)
	}
	return errors.New("invalid TOML")
}

func permissionRules(value any) ([]map[string]any, bool) {
	switch rules := value.(type) {
	case []map[string]any:
		return rules, true
	case []any:
		result := make([]map[string]any, 0, len(rules))
		for _, raw := range rules {
			rule, ok := raw.(map[string]any)
			if !ok {
				return nil, false
			}
			result = append(result, rule)
		}
		return result, true
	default:
		return nil, false
	}
}

func insertBytes(content []byte, offset int, addition string) []byte {
	result := make([]byte, 0, len(content)+len(addition))
	result = append(result, content[:offset]...)
	result = append(result, addition...)
	return append(result, content[offset:]...)
}

func tomlNewline(content []byte) string {
	if bytes.Contains(content, []byte("\r\n")) {
		return "\r\n"
	}
	return "\n"
}

func endsTOMLLine(content []byte) bool {
	return len(content) > 0 && content[len(content)-1] == '\n'
}

func appendTOMLBlock(content []byte, block, newline string) []byte {
	result := bytes.Clone(content)
	if len(result) > 0 {
		if !endsTOMLLine(result) {
			result = append(result, newline...)
		}
		if !bytes.HasSuffix(result, []byte(newline+newline)) {
			result = append(result, newline...)
		}
	}
	return append(result, block...)
}

type permissionSource struct {
	permissionHeaderAfter int
	allowOpen             int
	allowClose            int
	allowTrailingComma    bool
	rulesOpen             int
	rulesClose            int
	rulesTrailingComma    bool
}

func scanPermissionSource(content []byte) (permissionSource, error) {
	result := permissionSource{permissionHeaderAfter: -1, allowOpen: -1, allowClose: -1, rulesOpen: -1, rulesClose: -1}
	current := []string(nil)
	for offset := 0; offset < len(content); {
		offset = skipTOMLBlank(content, offset)
		if offset >= len(content) {
			break
		}
		if content[offset] == '[' {
			path, arrayTable, after, err := scanTOMLHeader(content, offset)
			if err != nil {
				return result, err
			}
			current = path
			if !arrayTable && reflect.DeepEqual(path, []string{"permission"}) {
				result.permissionHeaderAfter = after
			}
			offset = after
			continue
		}
		equals, err := scanTOMLEquals(content, offset)
		if err != nil {
			return result, err
		}
		key, ok := parseTOMLKey(content[offset:equals])
		if !ok {
			return result, errors.New("permission keys use a TOML shape the bounded updater cannot edit losslessly")
		}
		valueStart := skipTOMLHorizontal(content, equals+1)
		valueEnd, err := scanTOMLValue(content, valueStart)
		if err != nil {
			return result, err
		}
		full := append(append([]string(nil), current...), key...)
		if reflect.DeepEqual(full, []string{"permission", "allow"}) {
			if valueStart >= len(content) || content[valueStart] != '[' {
				return result, errors.New("permission.allow is not an array in source")
			}
			close, trailing, err := scanTOMLArray(content, valueStart)
			if err != nil {
				return result, err
			}
			result.allowOpen, result.allowClose, result.allowTrailingComma = valueStart, close, trailing
		}
		if reflect.DeepEqual(full, []string{"permission", "rules"}) {
			if valueStart >= len(content) || content[valueStart] != '[' {
				return result, errors.New("permission.rules is not an array in source")
			}
			close, trailing, err := scanTOMLArray(content, valueStart)
			if err != nil {
				return result, err
			}
			result.rulesOpen, result.rulesClose, result.rulesTrailingComma = valueStart, close, trailing
		}
		offset = valueEnd
	}
	return result, nil
}

func skipTOMLBlank(content []byte, offset int) int {
	if offset == 0 && bytes.HasPrefix(content, []byte{0xef, 0xbb, 0xbf}) {
		offset = 3
	}
	for offset < len(content) {
		switch content[offset] {
		case ' ', '\t', '\r', '\n':
			offset++
		case '#':
			for offset < len(content) && content[offset] != '\n' {
				offset++
			}
		default:
			return offset
		}
	}
	return offset
}

func skipTOMLHorizontal(content []byte, offset int) int {
	for offset < len(content) && (content[offset] == ' ' || content[offset] == '\t') {
		offset++
	}
	return offset
}

func scanTOMLHeader(content []byte, offset int) ([]string, bool, int, error) {
	arrayTable := offset+1 < len(content) && content[offset+1] == '['
	open := 1
	if arrayTable {
		open = 2
	}
	i := offset + open
	innerStart := i
	for i < len(content) {
		if content[i] == '\n' {
			return nil, false, 0, errors.New("unterminated TOML table header")
		}
		if content[i] == '\'' || content[i] == '"' {
			next, err := skipTOMLString(content, i)
			if err != nil {
				return nil, false, 0, err
			}
			i = next
			continue
		}
		if content[i] == ']' && (!arrayTable || i+1 < len(content) && content[i+1] == ']') {
			path, ok := parseTOMLKey(content[innerStart:i])
			if !ok {
				return nil, false, 0, errors.New("table header uses a TOML key shape the bounded updater cannot edit losslessly")
			}
			i++
			if arrayTable {
				i++
			}
			for i < len(content) && content[i] != '\n' {
				i++
			}
			if i < len(content) {
				i++
			}
			return path, arrayTable, i, nil
		}
		i++
	}
	return nil, false, 0, errors.New("unterminated TOML table header")
}

func scanTOMLEquals(content []byte, offset int) (int, error) {
	for i := offset; i < len(content); {
		switch content[i] {
		case '\n':
			return 0, errors.New("TOML assignment has no equals sign")
		case '\'', '"':
			next, err := skipTOMLString(content, i)
			if err != nil {
				return 0, err
			}
			i = next
		case '=':
			return i, nil
		default:
			i++
		}
	}
	return 0, errors.New("TOML assignment has no equals sign")
}

func scanTOMLValue(content []byte, offset int) (int, error) {
	square, curly := 0, 0
	for i := offset; i < len(content); {
		switch content[i] {
		case '\'', '"':
			next, err := skipTOMLString(content, i)
			if err != nil {
				return 0, err
			}
			i = next
		case '#':
			for i < len(content) && content[i] != '\n' {
				i++
			}
			if square == 0 && curly == 0 {
				return i, nil
			}
		case '[':
			square++
			i++
		case ']':
			square--
			i++
		case '{':
			curly++
			i++
		case '}':
			curly--
			i++
		case '\n':
			i++
			if square == 0 && curly == 0 {
				return i, nil
			}
		default:
			i++
		}
	}
	if square != 0 || curly != 0 {
		return 0, errors.New("unterminated TOML value")
	}
	return len(content), nil
}

func scanTOMLArray(content []byte, open int) (int, bool, error) {
	depth := 0
	last := byte(0)
	for i := open; i < len(content); {
		switch content[i] {
		case '\'', '"':
			last = 's'
			next, err := skipTOMLString(content, i)
			if err != nil {
				return 0, false, err
			}
			i = next
		case '#':
			for i < len(content) && content[i] != '\n' {
				i++
			}
		case '[':
			depth++
			if depth > 1 {
				last = '['
			}
			i++
		case ']':
			depth--
			if depth == 0 {
				return i, last == ',', nil
			}
			last = ']'
			i++
		default:
			if content[i] != ' ' && content[i] != '\t' && content[i] != '\r' && content[i] != '\n' {
				last = content[i]
			}
			i++
		}
	}
	return 0, false, errors.New("unterminated permission.allow array")
}

func skipTOMLString(content []byte, offset int) (int, error) {
	quote := content[offset]
	triple := offset+2 < len(content) && content[offset+1] == quote && content[offset+2] == quote
	i := offset + 1
	if triple {
		i += 2
	}
	for i < len(content) {
		if quote == '"' && content[i] == '\\' {
			i += 2
			continue
		}
		if content[i] != quote {
			i++
			continue
		}
		if !triple {
			return i + 1, nil
		}
		run := 0
		for i+run < len(content) && content[i+run] == quote {
			run++
		}
		if run >= 3 {
			return i + run, nil
		}
		i += run
	}
	return 0, errors.New("unterminated TOML string")
}

func parseTOMLKey(raw []byte) ([]string, bool) {
	parts := []string{}
	for offset := 0; ; {
		offset = skipTOMLHorizontal(raw, offset)
		if offset >= len(raw) {
			return nil, false
		}
		start := offset
		var part string
		if raw[offset] == '\'' || raw[offset] == '"' {
			end, err := skipTOMLString(raw, offset)
			if err != nil || end-offset >= 3 && raw[offset+1] == raw[offset] {
				return nil, false
			}
			var decoded map[string]any
			if _, err := toml.Decode("value = "+string(raw[offset:end]), &decoded); err != nil {
				return nil, false
			}
			part, _ = decoded["value"].(string)
			offset = end
		} else {
			for offset < len(raw) && isBareTOMLKey(raw[offset]) {
				offset++
			}
			if offset == start {
				return nil, false
			}
			part = string(raw[start:offset])
		}
		parts = append(parts, part)
		offset = skipTOMLHorizontal(raw, offset)
		if offset == len(raw) {
			return parts, true
		}
		if raw[offset] != '.' {
			return nil, false
		}
		offset++
	}
}

func isBareTOMLKey(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '_' || value == '-'
}

type grokConfigDestination struct {
	path       string
	info       os.FileInfo
	parentInfo os.FileInfo
}

func bindGrokConfigDestination(slot string) (grokConfigDestination, error) {
	abs, err := filepath.Abs(slot)
	if err != nil {
		return grokConfigDestination{}, err
	}
	path := abs
	if info, err := os.Lstat(abs); err == nil && info.Mode()&os.ModeSymlink != 0 {
		path, err = filepath.EvalSymlinks(abs)
		if err != nil {
			return grokConfigDestination{}, fmt.Errorf("resolve config symlink: %w", err)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return grokConfigDestination{}, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return grokConfigDestination{}, err
	}
	path = filepath.Join(parent, filepath.Base(path))
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return grokConfigDestination{}, err
	}
	info, err := os.Stat(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return grokConfigDestination{}, err
	}
	if err == nil && !info.Mode().IsRegular() {
		return grokConfigDestination{}, errors.New("config destination is not a regular file")
	}
	return grokConfigDestination{path: path, info: info, parentInfo: parentInfo}, nil
}

func proveGrokConfigDestination(slot string, expected grokConfigDestination) error {
	now, err := bindGrokConfigDestination(slot)
	if err != nil {
		return err
	}
	if now.path != expected.path || !os.SameFile(now.parentInfo, expected.parentInfo) || now.info == nil != (expected.info == nil) || now.info != nil && !os.SameFile(now.info, expected.info) {
		return errors.New("config destination changed during update")
	}
	return nil
}

func readGrokConfig(dest grokConfigDestination) ([]byte, error) {
	if dest.info == nil {
		return []byte{}, nil
	}
	return os.ReadFile(dest.path)
}

func writeGrokConfigAtomically(slot string, dest grokConfigDestination, original, content []byte) error {
	dir := filepath.Dir(dest.path)
	temporary, err := os.CreateTemp(dir, ".sessionbus-grok-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	defer cleanup()
	mode := os.FileMode(0600)
	if dest.info != nil {
		mode = dest.info.Mode().Perm()
	}
	if err = temporary.Chmod(mode); err == nil && dest.info != nil {
		err = preserveConfigOwner(temporary, dest.info)
	}
	if err == nil {
		_, err = temporary.Write(content)
	}
	if err == nil {
		err = temporary.Sync()
	}
	if closeErr := temporary.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = proveGrokConfigDestination(slot, dest); err != nil {
		return err
	}
	current, err := readGrokConfig(dest)
	if err != nil {
		return err
	}
	if !bytes.Equal(current, original) {
		return errors.New("config contents changed during update")
	}
	if err = os.Rename(temporaryPath, dest.path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = directory.Sync()
	return errors.Join(err, directory.Close())
}

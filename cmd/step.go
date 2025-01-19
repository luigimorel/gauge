package cmd

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/getgauge/gauge/config"
	"github.com/getgauge/gauge/execution"
	"github.com/getgauge/gauge/execution/rerun"
	"github.com/getgauge/gauge/gauge"
	"github.com/spf13/cobra"
)

var (
	stepCmd = &cobra.Command{
		Use:     "step [spec-file:line-number]",
		Short:   "Run a single step",
		Long:    `Run a single step.`,
		Example: `gauge step specs/example.spec:14`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := config.SetProjectRoot(args); err != nil {
				exit(err, "")
			}

			executeStep(cmd)
		},
		DisableAutoGenTag: true,
	}
)

func executeStep(cmd *cobra.Command) {
	loadEnvAndReinitLogger(cmd)
	ensureScreenshotsDir()

	args := cmd.Flags().Args()
	parts := strings.Split(args[0], ":")
	pathParts := strings.Split(parts[0], "/")
	specFile := []string{pathParts[0]}
	lineNo, _ := strconv.Atoi(parts[1])

	if !skipCommandSave {
		rerun.WritePrevArgs(os.Args)
	}

	installMissingPlugins(installPlugins, false)

	step := &gauge.Step{
		Value:     strings.Join(args, " "),
		LineText:  strings.Join(args, " "),
		LineNo:    lineNo,
		IsConcept: false,
	}

	exitCode := execution.ExecuteStep(step, specFile)
	if failSafe && exitCode != execution.ParseFailed {
		exitCode = 0
	}

	os.Exit(exitCode)
}

func init() {
	GaugeCmd.AddCommand(stepCmd)
}

func validateStepArgs(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("Please provide a spec file and line number in format: specs/filename.spec:line-number")
	}

	matched, err := regexp.MatchString(`^specs/[^:]+\.spec:\d+$`, args[0])
	if err != nil {
		return fmt.Errorf("Error validating format")
	}
	if !matched {
		return fmt.Errorf("Invalid format. Expected specs/filename.spec:line-number")
	}

	return nil
}

func getStepFromSpecFile(specFile string, lineNo int) (*gauge.Step, error) {
	content, err := os.ReadFile(specFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read spec file: %w", err)
	}

	lines := strings.Split(string(content), "\n")
	if lineNo <= 0 || lineNo > len(lines) {
		return nil, fmt.Errorf("line number %d is out of range (file has %d lines)", lineNo, len(lines))
	}

	stepText := strings.TrimSpace(lines[lineNo-1])

	return &gauge.Step{
		Value:     stepText,
		LineText:  stepText,
		LineNo:    lineNo,
		IsConcept: false,
		FileName:  specFile,
	}, nil
}

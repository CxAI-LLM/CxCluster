<?php

declare(strict_types=1);

namespace CxAI\CxPHP\Cluster;

/**
 * NVIDIA Container Toolkit integration for CxLLM Studio.
 *
 * Provides GPU detection, container runtime configuration, CDI (Container
 * Device Interface) spec generation, NIM container management, and GPU
 * health monitoring — all without shelling out unsafely.
 *
 * Architecture mirrors the NVIDIA Container Toolkit (nvidia-ctk) Go binary:
 *   - GPU discovery      → nvidia-smi / sysfs / /proc/driver/nvidia
 *   - Runtime config      → daemon.json / containerd config patching
 *   - CDI spec generation → JSON specs for nvidia.com/gpu devices
 *   - NIM orchestration   → Pulling & running NVIDIA NIM containers
 *   - Health monitoring    → GPU utilization, VRAM, temperature, ECC errors
 *
 * Environment variables:
 *   NVIDIA_VISIBLE_DEVICES    — GPU device list ('all', '0', '0,1', UUID)
 *   NVIDIA_DRIVER_CAPABILITIES — Comma-separated: compute,utility,graphics
 *   NVIDIA_CTK_PATH           — Path to nvidia-ctk binary (/usr/bin/nvidia-ctk)
 *   NVIDIA_NIM_BASE_URL       — Local NIM server URL (http://localhost:8000/v1)
 *   NVIDIA_NIM_IMAGE          — NIM container image (nvcr.io/nim/meta/llama-3.1-8b-instruct)
 *   NVIDIA_CONTAINER_RUNTIME  — Container runtime (nvidia, runc, crun)
 *   NVIDIA_CDI_ENABLED        — Enable CDI mode (true/false)
 *   NGC_CLI_API_KEY           — NGC registry authentication
 */
class NvidiaContainer
{
    // ── Constants ────────────────────────────────────────────────────────────

    /** Supported driver capabilities (from NVIDIA Container Runtime) */
    public const DRIVER_CAPABILITIES = [
        'compute',
        'compat32',
        'graphics',
        'utility',
        'video',
        'display',
        'ngx',
    ];

    /** Default capabilities when not specified */
    private const DEFAULT_CAPABILITIES = ['compute', 'utility'];

    /** CDI spec version */
    private const CDI_SPEC_VERSION = '0.6.0';

    /** CDI vendor/class */
    private const CDI_KIND = 'nvidia.com/gpu';

    /** GPU sysfs paths */
    private const SYSFS_GPU_PATH     = '/proc/driver/nvidia/gpus';
    private const SYSFS_VERSION_PATH = '/proc/driver/nvidia/version';

    /** Default NIM images by model family */
    private const NIM_IMAGES = [
        'llama-3.1-8b'   => 'nvcr.io/nim/meta/llama-3.1-8b-instruct:latest',
        'llama-3.1-70b'  => 'nvcr.io/nim/meta/llama-3.1-70b-instruct:latest',
        'mistral-nemo'   => 'nvcr.io/nim/nvidia/nemotron-mini-4b-instruct:latest',
        'nemotron-70b'   => 'nvcr.io/nim/meta/llama-3.3-70b-instruct:latest',
        'embedqa'        => 'nvcr.io/nim/nvidia/nv-embedqa-e5-v5:latest',
    ];

    /** GPU memory requirements per NIM model (in MB) */
    private const NIM_GPU_MEMORY = [
        'llama-3.1-8b'  => 16_384,
        'llama-3.1-70b' => 81_920,
        'mistral-nemo'  => 24_576,
        'nemotron-70b'  => 81_920,
        'embedqa'       => 8_192,
    ];

    // ── Instance State ───────────────────────────────────────────────────────

    private static ?self $instance = null;
    private bool $detected = false;

    /** @var list<array{index: int, uuid: string, name: string, memory_total_mb: int, memory_used_mb: int, memory_free_mb: int, utilization_gpu: int, utilization_memory: int, temperature: int, power_draw_w: float, power_limit_w: float, ecc_errors: int, driver_version: string, cuda_version: string, compute_capability: string}> */
    private array $gpus = [];

    private string $driverVersion = '';
    private string $cudaVersion   = '';
    private string $ctkPath;
    private string $containerRuntime;
    private bool   $cdiEnabled;

    /** @var list<string> */
    private array $visibleDevices;

    /** @var list<string> */
    private array $driverCapabilities;

    // ── Constructor ──────────────────────────────────────────────────────────

    public function __construct()
    {
        $this->ctkPath           = (string) (getenv('NVIDIA_CTK_PATH') ?: '/usr/bin/nvidia-ctk');
        $this->containerRuntime  = (string) (getenv('NVIDIA_CONTAINER_RUNTIME') ?: 'nvidia');
        $this->cdiEnabled        = in_array(
            strtolower((string) (getenv('NVIDIA_CDI_ENABLED') ?: '')),
            ['1', 'true', 'yes'],
            true,
        );

        $visibleRaw = (string) (getenv('NVIDIA_VISIBLE_DEVICES') ?: 'all');
        $this->visibleDevices = $visibleRaw === 'all' ? ['all'] : array_map('trim', explode(',', $visibleRaw));

        $capabilitiesRaw = (string) (getenv('NVIDIA_DRIVER_CAPABILITIES') ?: '');
        $this->driverCapabilities = $capabilitiesRaw !== ''
            ? array_intersect(array_map('trim', explode(',', $capabilitiesRaw)), self::DRIVER_CAPABILITIES)
            : self::DEFAULT_CAPABILITIES;
    }

    /** Singleton accessor. */
    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    // ── GPU Discovery ────────────────────────────────────────────────────────

    /**
     * Detect GPUs using nvidia-smi (XML output for structured parsing).
     *
     * @return list<array{index: int, uuid: string, name: string, memory_total_mb: int, memory_used_mb: int, memory_free_mb: int, utilization_gpu: int, utilization_memory: int, temperature: int, power_draw_w: float, power_limit_w: float, ecc_errors: int, driver_version: string, cuda_version: string, compute_capability: string}>
     */
    public function detectGpus(): array
    {
        if ($this->detected) {
            return $this->gpus;
        }
        $this->detected = true;

        // Try nvidia-smi CSV query (safer than XML for parsing)
        $cmd = 'nvidia-smi --query-gpu='
            . 'index,uuid,name,memory.total,memory.used,memory.free,'
            . 'utilization.gpu,utilization.memory,temperature.gpu,'
            . 'power.draw,power.limit,ecc.errors.corrected.volatile.total,'
            . 'driver_version,cuda_version,compute_cap'
            . ' --format=csv,noheader,nounits 2>/dev/null';

        $output = [];
        $exitCode = 0;
        exec($cmd, $output, $exitCode);

        if ($exitCode !== 0 || $output === []) {
            // Fallback: try sysfs
            return $this->detectFromSysfs();
        }

        foreach ($output as $line) {
            $fields = array_map('trim', explode(',', $line));
            if (count($fields) < 15) {
                continue;
            }

            $this->driverVersion = $fields[12];
            $this->cudaVersion   = $fields[13];

            $this->gpus[] = [
                'index'              => (int) $fields[0],
                'uuid'               => $fields[1],
                'name'               => $fields[2],
                'memory_total_mb'    => (int) $fields[3],
                'memory_used_mb'     => (int) $fields[4],
                'memory_free_mb'     => (int) $fields[5],
                'utilization_gpu'    => (int) $fields[6],
                'utilization_memory' => (int) $fields[7],
                'temperature'        => (int) $fields[8],
                'power_draw_w'       => (float) $fields[9],
                'power_limit_w'      => (float) $fields[10],
                'ecc_errors'         => (int) ($fields[11] === 'N/A' ? 0 : $fields[11]),
                'driver_version'     => $fields[12],
                'cuda_version'       => $fields[13],
                'compute_capability' => $fields[14],
            ];
        }

        // Filter by NVIDIA_VISIBLE_DEVICES
        if ($this->visibleDevices !== ['all'] && $this->gpus !== []) {
            $this->gpus = array_values(array_filter(
                $this->gpus,
                fn(array $gpu) => in_array((string) $gpu['index'], $this->visibleDevices, true)
                    || in_array($gpu['uuid'], $this->visibleDevices, true),
            ));
        }

        return $this->gpus;
    }

    /**
     * Fallback GPU discovery from /proc/driver/nvidia/gpus.
     *
     * @return list<array>
     */
    private function detectFromSysfs(): array
    {
        if (!is_dir(self::SYSFS_GPU_PATH)) {
            return [];
        }

        $entries = @scandir(self::SYSFS_GPU_PATH);
        if ($entries === false) {
            return [];
        }

        $index = 0;
        foreach ($entries as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $infoPath = self::SYSFS_GPU_PATH . "/{$entry}/information";
            $info = @file_get_contents($infoPath);
            if ($info === false) {
                continue;
            }

            $name = 'Unknown GPU';
            if (preg_match('/Model:\s*(.+)/', $info, $m)) {
                $name = trim($m[1]);
            }

            $this->gpus[] = [
                'index'              => $index,
                'uuid'               => $entry,
                'name'               => $name,
                'memory_total_mb'    => 0,
                'memory_used_mb'     => 0,
                'memory_free_mb'     => 0,
                'utilization_gpu'    => 0,
                'utilization_memory' => 0,
                'temperature'        => 0,
                'power_draw_w'       => 0.0,
                'power_limit_w'      => 0.0,
                'ecc_errors'         => 0,
                'driver_version'     => $this->getDriverVersion(),
                'cuda_version'       => '',
                'compute_capability' => '',
            ];
            $index++;
        }

        return $this->gpus;
    }

    /** @return bool True if at least one GPU is available. */
    public function hasGpu(): bool
    {
        return $this->detectGpus() !== [];
    }

    /** @return int Number of visible GPUs. */
    public function gpuCount(): int
    {
        return count($this->detectGpus());
    }

    /** @return string NVIDIA driver version or '' if unavailable. */
    public function getDriverVersion(): string
    {
        if ($this->driverVersion !== '') {
            return $this->driverVersion;
        }

        if (is_readable(self::SYSFS_VERSION_PATH)) {
            $ver = @file_get_contents(self::SYSFS_VERSION_PATH);
            if ($ver !== false && preg_match('/Kernel Module\s+([\d.]+)/', $ver, $m)) {
                $this->driverVersion = $m[1];
            }
        }

        return $this->driverVersion;
    }

    /** @return string CUDA version or '' if unavailable. */
    public function getCudaVersion(): string
    {
        if ($this->cudaVersion === '' && !$this->detected) {
            $this->detectGpus();
        }
        return $this->cudaVersion;
    }

    // ── CDI (Container Device Interface) ────────────────────────────────────

    /**
     * Generate a CDI spec for the detected GPUs.
     *
     * Mirrors `nvidia-ctk cdi generate` output — produces a JSON CDI spec
     * that container runtimes (containerd, CRI-O, Podman) use to inject
     * GPU device nodes and driver libraries into containers.
     *
     * @return array{cdiVersion: string, kind: string, devices: list<array>, containerEdits: array}
     */
    public function generateCdiSpec(): array
    {
        $gpus = $this->detectGpus();
        $devices = [];

        foreach ($gpus as $gpu) {
            $devices[] = [
                'name' => "gpu{$gpu['index']}",
                'containerEdits' => [
                    'deviceNodes' => [
                        ['path' => "/dev/nvidia{$gpu['index']}", 'hostPath' => "/dev/nvidia{$gpu['index']}"],
                        ['path' => '/dev/nvidiactl',             'hostPath' => '/dev/nvidiactl'],
                        ['path' => '/dev/nvidia-uvm',            'hostPath' => '/dev/nvidia-uvm'],
                        ['path' => '/dev/nvidia-uvm-tools',      'hostPath' => '/dev/nvidia-uvm-tools'],
                    ],
                    'env' => [
                        "NVIDIA_VISIBLE_DEVICES={$gpu['index']}",
                        'NVIDIA_DRIVER_CAPABILITIES=' . implode(',', $this->driverCapabilities),
                    ],
                ],
            ];
        }

        return [
            'cdiVersion'     => self::CDI_SPEC_VERSION,
            'kind'           => self::CDI_KIND,
            'devices'        => $devices,
            'containerEdits' => [
                'deviceNodes' => [
                    ['path' => '/dev/nvidiactl',        'hostPath' => '/dev/nvidiactl'],
                    ['path' => '/dev/nvidia-uvm',       'hostPath' => '/dev/nvidia-uvm'],
                    ['path' => '/dev/nvidia-uvm-tools', 'hostPath' => '/dev/nvidia-uvm-tools'],
                    ['path' => '/dev/nvidia-modeset',    'hostPath' => '/dev/nvidia-modeset'],
                ],
                'env' => [
                    'NVIDIA_DRIVER_CAPABILITIES=' . implode(',', $this->driverCapabilities),
                    'PATH=/usr/local/nvidia/bin:${PATH}',
                    'LD_LIBRARY_PATH=/usr/local/nvidia/lib64:${LD_LIBRARY_PATH}',
                ],
                'mounts' => [
                    [
                        'hostPath'      => '/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1',
                        'containerPath' => '/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1',
                        'options'       => ['ro', 'nosuid', 'nodev', 'bind'],
                    ],
                ],
            ],
        ];
    }

    // ── Container Runtime Config ─────────────────────────────────────────────

    /**
     * Generate Docker daemon.json fragment for NVIDIA runtime.
     *
     * Mirrors `nvidia-ctk runtime configure --runtime=docker`.
     *
     * @return array{runtimes: array, default-runtime?: string}
     */
    public function dockerRuntimeConfig(bool $setDefault = false): array
    {
        $config = [
            'runtimes' => [
                'nvidia' => [
                    'path' => '/usr/bin/nvidia-container-runtime',
                    'runtimeArgs' => [],
                ],
            ],
        ];

        if ($setDefault) {
            $config['default-runtime'] = 'nvidia';
        }

        return $config;
    }

    /**
     * Generate containerd config snippet for NVIDIA runtime class.
     *
     * Mirrors `nvidia-ctk runtime configure --runtime=containerd`.
     *
     * @return array{plugins: array}
     */
    public function containerdRuntimeConfig(): array
    {
        return [
            'plugins' => [
                'io.containerd.grpc.v1.cri' => [
                    'containerd' => [
                        'runtimes' => [
                            'nvidia' => [
                                'runtime_type'                    => 'io.containerd.runc.v2',
                                'privileged_without_host_devices' => false,
                                'options' => [
                                    'BinaryName' => '/usr/bin/nvidia-container-runtime',
                                ],
                            ],
                        ],
                    ],
                ],
            ],
        ];
    }

    // ── NIM Container Management ─────────────────────────────────────────────

    /**
     * Get the NIM image name for a given model family.
     *
     * @return string Full image reference (e.g. nvcr.io/nim/meta/llama-3.1-8b-instruct:latest)
     */
    public function nimImage(string $modelFamily): string
    {
        $env = (string) getenv('NVIDIA_NIM_IMAGE');
        if ($env !== '') {
            return $env;
        }
        return self::NIM_IMAGES[$modelFamily] ?? self::NIM_IMAGES['llama-3.1-8b'];
    }

    /**
     * Check if the system has enough GPU memory to run a NIM model.
     *
     * @return array{sufficient: bool, required_mb: int, available_mb: int, model: string}
     */
    public function checkNimRequirements(string $modelFamily): array
    {
        $required = self::NIM_GPU_MEMORY[$modelFamily] ?? 16_384;
        $gpus = $this->detectGpus();

        $totalFreeMb = 0;
        foreach ($gpus as $gpu) {
            $totalFreeMb += $gpu['memory_free_mb'];
        }

        return [
            'sufficient'   => $totalFreeMb >= $required,
            'required_mb'  => $required,
            'available_mb' => $totalFreeMb,
            'model'        => $modelFamily,
            'gpu_count'    => count($gpus),
        ];
    }

    /**
     * Generate `docker run` command for a NIM container.
     *
     * @return string Ready-to-execute shell command
     */
    public function nimRunCommand(string $modelFamily, int $port = 8000): string
    {
        $image = $this->nimImage($modelFamily);
        $gpu   = $this->visibleDevices === ['all'] ? 'all' : implode(',', $this->visibleDevices);

        return sprintf(
            'docker run -d --name nim-%s --gpus %s -p %d:8000 '
                . '-e NGC_CLI_API_KEY=%s '
                . '-v nim-cache:/opt/nim/.cache '
                . '%s',
            escapeshellarg($modelFamily),
            escapeshellarg($gpu),
            $port,
            '${NGC_CLI_API_KEY}',
            escapeshellarg($image),
        );
    }

    /**
     * Generate docker-compose service definition for a NIM container.
     *
     * @return array{image: string, ports: list<string>, environment: list<string>, deploy: array, volumes: list<string>}
     */
    public function nimComposeService(string $modelFamily, int $port = 8000): array
    {
        $image = $this->nimImage($modelFamily);
        $required = self::NIM_GPU_MEMORY[$modelFamily] ?? 16_384;

        return [
            'image'       => $image,
            'ports'       => ["{$port}:8000"],
            'environment' => [
                'NGC_CLI_API_KEY=${NGC_CLI_API_KEY}',
            ],
            'deploy' => [
                'resources' => [
                    'reservations' => [
                        'devices' => [
                            [
                                'driver'       => 'nvidia',
                                'count'        => ($required > 40_000) ? 2 : 1,
                                'capabilities' => ['gpu'],
                            ],
                        ],
                    ],
                ],
            ],
            'volumes' => ['nim-cache:/opt/nim/.cache'],
            'healthcheck' => [
                'test'     => ['CMD', 'curl', '-sf', 'http://localhost:8000/v1/health/ready'],
                'interval' => '30s',
                'timeout'  => '10s',
                'retries'  => 5,
            ],
            'restart' => 'unless-stopped',
        ];
    }

    // ── GPU Health Monitoring ────────────────────────────────────────────────

    /**
     * Full GPU health report.
     *
     * @return array{available: bool, gpu_count: int, driver_version: string, cuda_version: string, gpus: list<array>, toolkit: array, nim: array}
     */
    public function healthReport(): array
    {
        $gpus = $this->detectGpus();

        $warnings = [];
        foreach ($gpus as $gpu) {
            if ($gpu['temperature'] > 85) {
                $warnings[] = "GPU {$gpu['index']} ({$gpu['name']}): temperature {$gpu['temperature']}°C exceeds 85°C threshold";
            }
            if ($gpu['ecc_errors'] > 0) {
                $warnings[] = "GPU {$gpu['index']} ({$gpu['name']}): {$gpu['ecc_errors']} ECC errors detected";
            }
            if ($gpu['memory_total_mb'] > 0 && ($gpu['memory_used_mb'] / $gpu['memory_total_mb']) > 0.95) {
                $warnings[] = "GPU {$gpu['index']} ({$gpu['name']}): VRAM usage above 95%";
            }
        }

        $totalMemory = 0;
        $totalUsed   = 0;
        foreach ($gpus as $gpu) {
            $totalMemory += $gpu['memory_total_mb'];
            $totalUsed   += $gpu['memory_used_mb'];
        }

        return [
            'available'      => $gpus !== [],
            'gpu_count'      => count($gpus),
            'driver_version' => $this->driverVersion ?: $this->getDriverVersion(),
            'cuda_version'   => $this->cudaVersion,
            'total_memory_mb' => $totalMemory,
            'used_memory_mb'  => $totalUsed,
            'free_memory_mb'  => $totalMemory - $totalUsed,
            'gpus'           => $gpus,
            'warnings'       => $warnings,
            'toolkit' => [
                'ctk_path'             => $this->ctkPath,
                'ctk_available'        => is_executable($this->ctkPath),
                'container_runtime'    => $this->containerRuntime,
                'cdi_enabled'          => $this->cdiEnabled,
                'driver_capabilities'  => $this->driverCapabilities,
                'visible_devices'      => $this->visibleDevices,
            ],
            'nim' => [
                'base_url'  => (string) (getenv('NVIDIA_NIM_BASE_URL') ?: ''),
                'image'     => (string) (getenv('NVIDIA_NIM_IMAGE') ?: ''),
                'available' => (string) getenv('NVIDIA_NIM_BASE_URL') !== '',
            ],
        ];
    }

    /**
     * Quick GPU availability check (suitable for /health endpoint).
     *
     * @return array{available: bool, count: int, driver: string}
     */
    public function quickStatus(): array
    {
        return [
            'available' => $this->hasGpu(),
            'count'     => $this->gpuCount(),
            'driver'    => $this->getDriverVersion(),
        ];
    }

    /**
     * Assess GPU fitness for a specific NIM model.
     *
     * @return array{fit: bool, model: string, gpus_needed: int, gpus_available: int, memory_ok: bool, driver_ok: bool, reasons: list<string>}
     */
    public function assessFitness(string $modelFamily): array
    {
        $gpus     = $this->detectGpus();
        $required = self::NIM_GPU_MEMORY[$modelFamily] ?? 16_384;
        $reasons  = [];

        if ($gpus === []) {
            $reasons[] = 'No NVIDIA GPUs detected';
        }

        $totalFree = 0;
        foreach ($gpus as $gpu) {
            $totalFree += $gpu['memory_free_mb'];
        }
        $memoryOk = $totalFree >= $required;
        if (!$memoryOk && $gpus !== []) {
            $reasons[] = sprintf('Insufficient VRAM: %d MB free, %d MB required', $totalFree, $required);
        }

        // NIM generally needs compute capability >= 7.0 (Volta+)
        $driverOk = true;
        foreach ($gpus as $gpu) {
            if ($gpu['compute_capability'] !== '' && version_compare($gpu['compute_capability'], '7.0', '<')) {
                $driverOk = false;
                $reasons[] = "GPU {$gpu['index']} compute capability {$gpu['compute_capability']} < 7.0 (Volta minimum)";
            }
        }

        $gpusNeeded = ($required > 40_000) ? 2 : 1;

        return [
            'fit'            => $gpus !== [] && $memoryOk && $driverOk,
            'model'          => $modelFamily,
            'gpus_needed'    => $gpusNeeded,
            'gpus_available' => count($gpus),
            'memory_ok'      => $memoryOk,
            'driver_ok'      => $driverOk,
            'reasons'        => $reasons,
        ];
    }

    // ── Info & Helpers ───────────────────────────────────────────────────────

    /**
     * Full toolkit info (mirrors `nvidia-ctk info`).
     *
     * @return array{version: string, cdi: array, runtime: array, gpus: array}
     */
    public function info(): array
    {
        return [
            'version' => 'cxllm-nctk-1.0.0',
            'source'  => 'NVIDIA Container Toolkit integration for CxLLM Studio',
            'cdi' => [
                'enabled' => $this->cdiEnabled,
                'kind'    => self::CDI_KIND,
                'version' => self::CDI_SPEC_VERSION,
            ],
            'runtime' => [
                'name' => $this->containerRuntime,
                'path' => '/usr/bin/nvidia-container-runtime',
            ],
            'gpus'    => $this->quickStatus(),
            'nim'     => [
                'images'     => self::NIM_IMAGES,
                'gpu_memory' => self::NIM_GPU_MEMORY,
            ],
        ];
    }

    /**
     * Check if the NVIDIA container runtime is properly installed.
     *
     * @return array{installed: bool, components: array<string, bool>}
     */
    public function checkInstallation(): array
    {
        $components = [
            'nvidia-smi'                   => $this->commandExists('nvidia-smi'),
            'nvidia-container-runtime'     => $this->commandExists('nvidia-container-runtime'),
            'nvidia-container-toolkit'     => $this->commandExists('nvidia-container-toolkit'),
            'nvidia-ctk'                   => is_executable($this->ctkPath),
            'nvidia-container-cli'         => $this->commandExists('nvidia-container-cli'),
            'libnvidia-container'          => is_file('/usr/lib/x86_64-linux-gnu/libnvidia-container.so.1')
                || is_file('/usr/lib64/libnvidia-container.so.1'),
        ];

        return [
            'installed'  => $components['nvidia-smi'] && ($components['nvidia-container-runtime'] || $components['nvidia-container-toolkit']),
            'components' => $components,
        ];
    }

    /** Reset the singleton (for testing). */
    public static function reset(): void
    {
        self::$instance = null;
    }

    /** Check if a command exists on PATH. */
    private function commandExists(string $cmd): bool
    {
        $result = [];
        $exitCode = 0;
        exec(sprintf('command -v %s 2>/dev/null', escapeshellarg($cmd)), $result, $exitCode);
        return $exitCode === 0;
    }
}

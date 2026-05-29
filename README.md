# Trabalho Pratico 2 - Transferencia de Arquivos Peer-to-Peer

Implementacao em Python 3 de um sistema elementar de transferencia P2P, no qual cada peer executa simultaneamente:

- um servidor TCP para responder `META`, `HAVE` e `GET`;
- um cliente que consulta vizinhos estaticos e solicita blocos ausentes;
- validacao de SHA-256 por bloco e do arquivo final;
- remontagem do arquivo quando todos os blocos sao recebidos.

A solucao usa apenas a biblioteca padrao do Python.

## Estrutura

```text
p2p_transfer/peer.py         # peer P2P
p2p_transfer/test_runner.py  # executor dos estudos de caso locais
run_demo.ps1                 # demo PowerShell com limpeza, execucao e logs
README.md                    # instrucoes de execucao
```

## Requisitos

- Python 3 instalado e disponivel no comando `python`
- PowerShell para executar a demo automatizada

Se quiser instalar dependencias listadas no projeto:

```bash
pip install -r requirements.txt
```

## Execucao recomendada

A forma mais simples de demonstrar o projeto e ver a transferencia acontecendo com logs dos dois peers no mesmo terminal e:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_demo.ps1
```

Esse script:

- cria `run/arquivo_original.bin` se ele ainda nao existir;
- limpa `run/peer_A` e `run/peer_B`;
- sobe o seeder `A` e o leecher `B`;
- mostra os logs dos dois peers no mesmo terminal;
- encerra o seeder ao fim da transferencia.

Ao terminar:

- o arquivo baixado fica em `run/peer_B/downloads/arquivo_original.bin`
- os logs completos ficam em:
  - `run/peer_A_stdout.log`
  - `run/peer_A_stderr.log`
  - `run/peer_B_stdout.log`
  - `run/peer_B_stderr.log`

## Saida esperada da demo

Durante a execucao, voce deve ver linhas como:

- `Seeder inicial`
- `META recebido`
- `RECEIVED bloco=...`
- `ASSEMBLED arquivo=...`
- `Transferencia concluida com sucesso.`

## Execucao dos testes

Para rodar um conjunto rapido de testes:

```bash
python -m p2p_transfer.test_runner --quick --verbose
```

Para rodar os estudos de caso completos:

```bash
python -m p2p_transfer.test_runner --verbose
```

Os resultados e arquivos gerados ficam em `test_runs/`.

## Execucao manual de dois peers

Essa opcao e util se voce quiser inspecionar o comportamento de cada peer separadamente.

Primeiro, crie um arquivo de teste:

```bash
python -c "from pathlib import Path; p = Path('run/arquivo_original.bin'); p.parent.mkdir(parents=True, exist_ok=True); p.write_bytes(bytes(range(256)) * 256)"
```

Em um terminal, inicie o seeder:

```bash
python -m p2p_transfer.peer \
  --peer-id A \
  --host 127.0.0.1 \
  --port 5001 \
  --neighbors 127.0.0.1:5002 \
  --data-dir ./run/peer_A \
  --file ./run/arquivo_original.bin \
  --target arquivo_original.bin \
  --block-size 1024 \
  --serve-only
```

Em outro terminal, inicie o leecher:

```bash
python -m p2p_transfer.peer \
  --peer-id B \
  --host 127.0.0.1 \
  --port 5002 \
  --neighbors 127.0.0.1:5001 \
  --data-dir ./run/peer_B \
  --target arquivo_original.bin \
  --block-size 1024 \
  --exit-when-complete
```

No PowerShell, os comandos equivalentes sao:

Seeder:

```powershell
python -m p2p_transfer.peer `
  --peer-id A `
  --host 127.0.0.1 `
  --port 5001 `
  --neighbors 127.0.0.1:5002 `
  --data-dir ./run/peer_A `
  --file ./run/arquivo_original.bin `
  --target arquivo_original.bin `
  --block-size 1024 `
  --serve-only
```

Leecher:

```powershell
python -m p2p_transfer.peer `
  --peer-id B `
  --host 127.0.0.1 `
  --port 5002 `
  --neighbors 127.0.0.1:5001 `
  --data-dir ./run/peer_B `
  --target arquivo_original.bin `
  --block-size 1024 `
  --exit-when-complete
```

O leecher pode receber os metadados automaticamente via mensagem `META`. Tambem e possivel passar `--meta caminho/arquivo.meta.json`.

## Protocolo usado

As mensagens de controle sao JSON em UTF-8 terminadas por quebra de linha. O bloco em si e enviado como carga binaria apos o cabecalho `BLOCK`.

- `META`: solicita metadados do arquivo
- `HAVE`: solicita o mapa de blocos disponiveis no vizinho
- `GET`: solicita um bloco especifico
- `GET_MANY`: solicita um lote de blocos para reduzir custo de abertura de conexoes
- `BLOCK`: retorna um bloco e seu hash SHA-256
- `BLOCKS`: retorna varios blocos em sequencia, cada um com tamanho e hash no cabecalho

## Observacoes

- O tracker e opcional no enunciado; esta implementacao usa lista estatica de vizinhos.
- Um peer que recebe e valida um bloco passa a servi-lo imediatamente pelo servidor local.
- O arquivo final e montado apenas quando todos os blocos conferem com os hashes do metadado.

## Resultados locais

Os estudos de caso executados localmente em `test_runs/summary.json` resultaram em status `OK` nos cenarios avaliados. O teste com 4 peers demonstrou compartilhamento entre leechers, com blocos recebidos de A, B, C e D.

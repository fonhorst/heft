from core.examples.GaHeftExecutorExample import GaHeftExecutorExample

obj = GaHeftExecutorExample()
wf_name = 'CyberShake_30'
reliability = 0.95
obj.main(reliability, False, wf_name)
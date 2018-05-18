import os
import logging
import time
import json
import re

from solc import compile_source, install_solc


logger = logging.getLogger(__name__)



def compile_contract(source, contract_name, compiler_version,
                     optimization_enambled, optimizer_runs):
    solc_bin = get_solc_bin_path(compiler_version)
    try:
        res = compile_source(source, output_values=['abi', 'bin', 'metadata'], solc_binary=solc_bin,
                             optimize=optimization_enambled, optimize_runs=optimizer_runs)
    except:
        logger.exception('Compilation error')
        raise RuntimeError('Compilation failed')
    try:
        contract_res = res['<stdin>:{}'.format(contract_name)]
    except KeyError:
        contract_res = res[contract_name]
    return contract_res


def get_solc_bin_path(compiler_version):
    commit = compiler_version.split('.')[-1]
    return os.path.expanduser('~/.py-solc/{}/bin/solc'.format(commit))
    # return os.path.expanduser('~/.py-solc/solc-{}/bin/solc'.format(compiler_version))



INSTALL_HARD_LIMIT = 60 * 15


def wait_install_solc(identifier):
    guard_path = '/tmp/solc_install_guard_{}'.format(identifier)
    start_time = time.time()

    if os.path.exists(guard_path):
        logger.debug('Solc install guard %s exists, wait installation finish', identifier)
        while os.path.exists(guard_path):
            time.sleep(5)
            if time.time() - start_time > INSTALL_HARD_LIMIT:
                logger.debug('Solc %s install wait aborted', identifier)
                return False
        logger.debug('Solc install guard %s removed, installation done', identifier)
        return True

    with open(guard_path, 'a'):
        pass
    logger.debug('Installing solc, version %s', identifier)
    try:
        install_solc(identifier)
    finally:
        os.unlink(guard_path)
    logger.debug('Solc installed, version %s, time %s', identifier, time.time() - start_time)
    return True


def cut_contract_metadata_hash(byte_code):
    """
    https://github.com/ethereum/solidity/blob/c9bdbcf470f4ca7f8d2d71f1be180274f534888d/libsolidity/interface/CompilerStack.cpp#L699

    bytes cborEncodedHash =
        // CBOR-encoding of the key "bzzr0"
        bytes{0x65, 'b', 'z', 'z', 'r', '0'}+
        // CBOR-encoding of the hash
        bytes{0x58, 0x20} + dev::swarmHash(metadata).asBytes();
    bytes cborEncodedMetadata;
    if (onlySafeExperimentalFeaturesActivated(_contract.sourceUnit().annotation().experimentalFeatures))
        cborEncodedMetadata =
            // CBOR-encoding of {"bzzr0": dev::swarmHash(metadata)}
            bytes{0xa1} +
            cborEncodedHash;
    else
        cborEncodedMetadata =
            // CBOR-encoding of {"bzzr0": dev::swarmHash(metadata), "experimental": true}
            bytes{0xa2} +
            cborEncodedHash +
            bytes{0x6c, 'e', 'x', 'p', 'e', 'r', 'i', 'm', 'e', 'n', 't', 'a', 'l', 0xf5};
    solAssert(cborEncodedMetadata.size() <= 0xffff, "Metadata too large");
    """

    cbor_hash = '65627a7a72305820(.*)'
    sig_1 = 'a1' + cbor_hash + '0029'
    sig_2 = 'a2' + cbor_hash + '6c6578706572696d656e74616cf50037'

    m = re.match('.*({}).*'.format(sig_1), byte_code)
    if m:
        logger.debug('Metadata signature found: %s', m.groups()[0])
        swarm_hash = m.groups()[1]
        return re.sub(swarm_hash, '', byte_code), swarm_hash

    m = re.match('.*({}).*'.format(sig_2), byte_code)
    if m:
        logger.debug('Metadata experimental signature found: %s', m.groups()[0])
        swarm_hash = m.groups()[1]
        return re.sub(swarm_hash, '', byte_code), swarm_hash
    logger.debug('No metadata hash found')
    return byte_code, ''



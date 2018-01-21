pragma solidity 0.4.18;
import 'SafeMath.sol';
import 'Math.sol';
import 'RLP.sol';
import 'Merkle.sol';
import 'Validate.sol';
import 'PriorityQueue.sol';


contract RootChain {
    using SafeMath for uint256;
    using RLP for bytes;
    using RLP for RLP.RLPItem;
    using RLP for RLP.Iterator;
    using Merkle for bytes32;

    /*
     * Events
     */
    event Deposit(address depositor, uint256 amount);

    /*
     *  Storage
     */
    /*
    childChain: A list of Plasma blocks, for each block storing (i) the Merkle root, (ii) the time the Merkle root was submitted.
    */
    mapping(uint256 => childBlock) public childChain;

    /*
    A list of submitted exit transactions, storing
     (i) the submitter address, and 
     (ii) the UTXO position (Plasma block number, txindex, outindex). 
    This must be stored in a data structure that allows transactions to be popped from the set in order of priority.
   */
    mapping(uint256 => exit) public exits;
    mapping(uint256 => uint256) public exitIds;
    PriorityQueue exitsQueue;

    // owner (set at initialization time)
    address public authority;

    
    uint256 public currentChildBlock;
    uint256 public lastParentBlock;
    uint256 public recentBlock;   // not used
    uint256 public weekOldBlock;  // updated in incrementOldBlocks [when submitBlock]

    struct exit {
        address owner;
        uint256 amount;
        uint256[3] utxoPos;
    }

    struct childBlock {
        bytes32 root;
        uint256 created_at;
    }

    /*
     *  Modifiers
     */
    modifier isAuthority() {
        require(msg.sender == authority);
        _;
    }

    modifier incrementOldBlocks() {
        while (childChain[weekOldBlock].created_at < block.timestamp.sub(1 weeks)) {
            if (childChain[weekOldBlock].created_at == 0) 
                break;
            weekOldBlock = weekOldBlock.add(1);
        }
        _;
    }

    function RootChain()
        public
    {
        authority = msg.sender;
        currentChildBlock = 1;
        lastParentBlock = block.number;
        exitsQueue = new PriorityQueue();
    }

    /* 

    Plasma Chain block submission: A Plasma block can be created in
    one of two ways. First, the operator of the Plasma chain can
    create blocks. Second, anyone can deposit any quantity of ETH into
    the chain, and when they do so the contract adds to the chain a
    block that contains exactly one transaction, creating a new UTXO
    with denomination equal to the amount that they deposit.

    Each Merkle root should be a root of a tree with depth-16 leaves, where each leaf is a transaction. 

    A transaction is an RLP-encoded object of the form:

      [blknum1, txindex1, oindex1, sig1, # Input 1
       blknum2, txindex2, oindex2, sig2, # Input 2
       newowner1, denom1,                # Output 1
       newowner2, denom2,                # Output 2
       fee]

    Each transaction has 2 inputs and 2 outputs, and the sum of the denominations of the outputs plus the fee must equal the sum of the denominations of the inputs. 
    
    The signatures must be signatures of all the other fields in the transaction, with the private key corresponding to the owner of that particular output. 

    A deposit block has all input fields, and the fields for the second output, zeroed out. 
    To make a transaction that spends only one UTXO, a user can zero out all fields for the second input.
    */
    function submitBlock(bytes32 root)
        public
        isAuthority
        incrementOldBlocks
    {
        require(block.number >= lastParentBlock.add(6));
        childChain[currentChildBlock] = childBlock({
            root: root,
            created_at: block.timestamp
        });
        currentChildBlock = currentChildBlock.add(1);
        lastParentBlock = block.number;
    }

    /*
     generates a block that contains only one transaction, generating a new UTXO into existence with denomination equal to the msg.value deposited
      txList[0-5, 9]: 0
      txList[6]: toAddress
      txList[7]: msg.value
     */
    function deposit(bytes txBytes)
        public
        payable
    {
        var txList = txBytes.toRLPItem().toList();
        require(txList.length == 11);
        for (uint256 i; i < 6; i++) {
            require(txList[i].toUint() == 0);
        }
        require(txList[7].toUint() == msg.value); // has to match!
        require(txList[9].toUint() == 0);
        bytes32 zeroBytes;
        // generate root through a lot of Keccak hashing
        bytes32 root = keccak256(keccak256(txBytes), new bytes(130));
        for (i = 0; i < 16; i++) {
            root = keccak256(root, zeroBytes);
            zeroBytes = keccak256(zeroBytes, zeroBytes);
        }
        // make a new block 
        childChain[currentChildBlock] = childBlock({
            root: root,
            created_at: block.timestamp
        });
        currentChildBlock = currentChildBlock.add(1);
        Deposit(txList[6].toAddress(), txList[7].toUint());
    }

    function getChildChain(uint256 blockNumber)
        public
        view
        returns (bytes32, uint256)
    {
        return (childChain[blockNumber].root, childChain[blockNumber].created_at);
    }

    function getExit(uint256 priority)
        public
        view
        returns (address, uint256, uint256[3])
    {
        return (exits[priority].owner, exits[priority].amount, exits[priority].utxoPos);
    }

    /*
    startExit(uint256 plasmaBlockNum, uint256 txindex, uint256 oindex, bytes tx, bytes proof, bytes confirmSig): 
     starts an exit procedure for a given UTXO. Requires as input
     (i) the Plasma block number and tx index in which the UTXO was created, 
     (ii) the output index, 
     (iii) the transaction containing that UTXO, 
     (iv) a Merkle proof of the transaction, and
     (v) a confirm signature from each of the previous owners of the now-spent outputs that were used to create the UTXO.
    */
    function startExit(uint256[3] txPos, bytes txBytes, bytes proof, bytes sigs)
        public
        incrementOldBlocks
    {
        var txList = txBytes.toRLPItem().toList();
/*
    A transaction is an RLP-encoded object (length 11) of the form: (sig1, sig2 is skipped for now)

      [0: blknum1, 1: txindex1, 2: oindex1, (sig1) # Input 1 
       3: blknum2, 4: txindex2, oindex2, (sig2) # Input 2
       6: newowner1, denom1,                 # Output 1
       8: newowner2, denom2,                 # Output 2
       10: fee]
*/
        require(txList.length == 11);
        require(msg.sender == txList[6 + 2 * txPos[2]].toAddress());
        bytes32 txHash = keccak256(txBytes);
        bytes32 merkleHash = keccak256(txHash, ByteUtils.slice(sigs, 0, 130));
        uint256 inputCount = txList[3].toUint() * 1000000 + txList[0].toUint();
        require(Validate.checkSigs(txHash, childChain[txPos[0]].root, inputCount, sigs));
        require(merkleHash.checkMembership(txPos[1], childChain[txPos[0]].root, proof));
        // arrange exits into a priority queue structure, where priority is normally the tuple (blknum, txindex, oindex) (alternatively, blknum * 1000000000 + txindex * 10000 + oindex). 
        // txPos[0] - blocknum
        // txPos[1] - txindex
        // txPos[2] - oindex
        uint256 priority = 1000000000 + txPos[1] * 10000 + txPos[2];
        uint256 exitId = txPos[0].mul(priority);
        priority = priority.mul(Math.max(txPos[0], weekOldBlock));
        require(exitIds[exitId] == 0);
        require(exits[priority].amount == 0);
        exitIds[exitId] = priority;
        exitsQueue.insert(priority);
        /*
        However, if when calling exit, the block that the UTXO was created in is more than 7 days old, then the blknum of the oldest Plasma block that is less than 7 days old is used instead. There is a
        passive loop that finalizes exits that are more than 14 days old, always processing exits in order of priority (earlier to later).

        This mechanism ensures that ordinarily, exits from earlier UTXOs are processed before exits from older UTXOs, 
        and particularly, if an attacker makes a invalid block containing bad UTXOs, the holders of all earlier UTXOs 
        will be able to exit before the attacker. 

        The 7 day minimum ensures that even for very old UTXOs, there is ample time to challenge them.
        */
        exits[priority] = exit({
            owner: txList[6 + 2 * txPos[2]].toAddress(),
            amount: txList[7 + 2 * txPos[2]].toUint(),
            utxoPos: txPos
        });
    }

    /*
    challengeExit(uint256 exitId, uint256 plasmaBlockNum, uint256 txindex, uint256 oindex, bytes tx, bytes proof, bytes confirmSig): 
    challenges an exit attempt in process, by providing a proof that the TXO was spent, 
    the spend was included in a block, and the owner made a confirm signature.
    */
    function challengeExit(uint256 exitId, uint256[3] txPos, bytes txBytes, bytes proof, bytes sigs, bytes confirmationSig)
        public
    {
        var txList = txBytes.toRLPItem().toList();
        require(txList.length == 11);
        uint256 priority = exitIds[exitId];
        uint256[3] memory exitsUtxoPos = exits[priority].utxoPos;
        require(exitsUtxoPos[0] == txList[0 + 2 * exitsUtxoPos[2]].toUint());
        require(exitsUtxoPos[1] == txList[1 + 2 * exitsUtxoPos[2]].toUint());
        require(exitsUtxoPos[2] == txList[2 + 2 * exitsUtxoPos[2]].toUint());
        var txHash = keccak256(txBytes);
        var confirmationHash = keccak256(txHash, sigs, childChain[txPos[0]].root);
        var merkleHash = keccak256(txHash, sigs);
        address owner = exits[priority].owner;
        require(owner == ECRecovery.recover(confirmationHash, confirmationSig));
        require(merkleHash.checkMembership(txPos[1], childChain[txPos[0]].root, proof));
        delete exits[priority];
        delete exitIds[exitId];
    }

    function finalizeExits()
        public
        incrementOldBlocks
        returns (uint256)
    {
        uint256 twoWeekOldTimestamp = block.timestamp.sub(2 weeks);
        exit memory currentExit = exits[exitsQueue.getMin()];
        while (childChain[currentExit.utxoPos[0]].created_at < twoWeekOldTimestamp && exitsQueue.currentSize() > 0) {
            // return childChain[currentExit.utxoPos[0]].created_at;
            uint256 exitId = currentExit.utxoPos[0] * 1000000000 + currentExit.utxoPos[1] * 10000 + currentExit.utxoPos[2];
            currentExit.owner.transfer(currentExit.amount);
            uint256 priority = exitsQueue.delMin();
            delete exits[priority];
            delete exitIds[exitId];
            currentExit = exits[exitsQueue.getMin()];
        }
    }
}

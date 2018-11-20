    /* Compiler version */
pragma solidity ^0.4.18;

contract Coin {

    mapping (address => uint) public balances;
    event Send(address from, address to, uint value);

    /* constructor, will be called only once, when the contract is deployed to the network */
    function Coin(uint initialSupply) {
        balances[msg.sender] = initialSupply;
        Send(address(0), msg.sender, initialSupply);
    }

    /* simple function for transfering the coin */
    /* Just for testing purpose, will add more error handling check in the future */
    function send(address _to, uint _value) returns (bool success) {
        if (balances[msg.sender] < _value) revert();

        balances[msg.sender] -= _value;
        balances[_to] += _value;
        Send(msg.sender, _to, _value);
        return true;
    }

    function balanceOf(address owner) public constant returns (uint balance) {
        return balances[owner];
    }

}